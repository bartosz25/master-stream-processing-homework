import datetime
from typing import Any, Dict, Iterable

import pandas
from pyspark.sql.streaming.state import GroupState


def generate_visit_output(user_id_tuple: Any,
                          visit_events: Iterable[pandas.DataFrame],
                          current_state: GroupState) -> Iterable[pandas.DataFrame]:
    session_expiration_time_16min_as_ms = 16 * 60 * 1000
    user_id = user_id_tuple[0]

    def get_visit_duration_to_return(state_to_output: GroupState) -> Dict[str, Any]:
        first_event_time_as_milliseconds_for_id, pages_from_state,\
            browser_code_output, browser_version_output = state_to_output
        sorted_visits = sorted(pages_from_state,
                               key=lambda event_time_with_page: event_time_with_page['eventTimeAsMilliseconds'])
        start_time = sorted_visits[0]['eventTimeAsMilliseconds']
        end_time = sorted_visits[len(sorted_visits) - 1]['eventTimeAsMilliseconds']

        navigation = []
        for i in range(0, len(sorted_visits)-1):
            current_visit = sorted_visits[i]
            next_visit = sorted_visits[i + 1]
            time_spent = next_visit.eventTimeAsMilliseconds - current_visit.eventTimeAsMilliseconds
            navigation.append({'page': current_visit.visitedPage, 'timeSpent': time_spent})

        return {
            "sessionId": [f'{user_id}_{first_event_time_as_milliseconds_for_id}'],
            "userId": [user_id],
            "startTime": [datetime.datetime.fromtimestamp(start_time / 1000.0, tz=datetime.timezone.utc)],
            "endTime": [datetime.datetime.fromtimestamp(end_time / 1000.0, tz=datetime.timezone.utc)],
            "navigation": [navigation],
            "browserCode": [browser_code_output],
            "browserVersion": [browser_version_output]
        }

    visit_to_return = None
    if current_state.hasTimedOut:
        print(f"Session ({current_state.get}) expired for {user_id}; let's generate the final output here")
        visit_to_return = get_visit_duration_to_return(current_state.get)
        current_state.remove()
    else:
        should_use_event_time_for_watermark = current_state.getCurrentWatermarkMs() == 0
        base_watermark = current_state.getCurrentWatermarkMs()
        new_pages = []
        first_event_time_as_milliseconds = 0
        browser_code = ''
        browser_version = ''
        for input_df_for_group in visit_events:
            browser_code = input_df_for_group['browserKey'].iloc[0]
            browser_version = input_df_for_group['browserVersion'].iloc[0]
            input_df_for_group['eventTimeAsMilliseconds'] = input_df_for_group['eventTime'] \
                .apply(lambda x: int(pandas.Timestamp(x).timestamp()) * 1000)
            first_event_time_as_milliseconds = int(input_df_for_group['eventTimeAsMilliseconds'].min())
            if should_use_event_time_for_watermark:
                base_watermark = int(input_df_for_group['eventTimeAsMilliseconds'].max())

            new_pages = input_df_for_group[['visitedPage', 'eventTimeAsMilliseconds']].to_dict(orient='records')

        old_pages_from_state = []
        if current_state.exists:
            first_event_time_as_milliseconds, old_pages_from_state, browser_code, browser_version = current_state.get

        pages_for_state = new_pages + old_pages_from_state
        current_state.update((first_event_time_as_milliseconds, pages_for_state,
                              browser_code, browser_version,))

        timeout_timestamp = base_watermark + session_expiration_time_16min_as_ms
        current_state.setTimeoutTimestamp(timeout_timestamp)

    if visit_to_return:
        yield pandas.DataFrame(visit_to_return)
