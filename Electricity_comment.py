import pandas as pd
from deutschland import smard
from pprint import pprint
from deutschland.smard.api import default_api


configuration = smard.Configuration(
    host="http://www.smard.de/app/chart_data",  # host/path given in example is wrong
    discard_unknown_keys=True  # suppress "deutschland.smard.exceptions.ApiTypeError: Invalid type for variable '1'.
    # Required value type is float and passed type was NoneType at ['received_data']['series'][140][1]"
)


def run():
    with smard.ApiClient(configuration) as api_client:
        api_instance = default_api.DefaultApi(api_client)
        smard_filter = 1223
        smard_region = "DE"
        smard_resolution = "quarterhour"

        try:
            # this will return all timestamps that can be used in smard_timestamp
            api_response = api_instance.filter_region_index_resolution_json_get(
                smard_filter, region=smard_region, resolution=smard_resolution)

            timestamp_df = pd.Series(api_response["timestamps"])
            last_timestamp_ms = timestamp_df.iloc[-1]
            last_datetime = pd.to_datetime(last_timestamp_ms, unit="ms")
            last_datetime = last_datetime.tz_localize('UTC')
            print("Latest json data starts at unixtimestamp %u ms (%s)"
                  % (last_timestamp_ms, last_datetime.tz_convert('Europe/Berlin')))

            smard_timestamp = int(last_timestamp_ms)
            api_response = api_instance.filter_region_filter_copy_region_copy_resolution_timestamp_json_get(
                smard_filter, smard_filter, smard_region, smard_timestamp,
                region=smard_region, resolution=smard_resolution)

            pprint(api_response)
        except smard.ApiException as e:
            print("SMART Api Exception filter_region_filter_copy_region_copy_resolution_timestamp_json_get: %s\n" % e)


if __name__ == "__main__":
    run()
