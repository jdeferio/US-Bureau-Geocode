import pandas as pd
import requests
import logging
import time

logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# ---------------- CONFIGURATION ---------------------------

# Backoff time sets how many minutes to wait between US Census Bureau pings when your API limit is hit
BACKOFF_TIME = 30
# Set your output file here.
output_filename = '/Users/JosephDeFerio/Desktop/test_x.csv'
# Set your input file name here.
input_filename = '/Users/JosephDeFerio/Desktop/test.csv'
# Specify the colun name in your input data that contains address here
address_column_name = "Address"


# -------------------------- DATA LOADING -----------------------

# Read the data to a Pandas DataFrame
data = pd.read_csv(input_filename, encoding='utf-8')

if address_column_name not in data.columns:
    raise ValueError("Missing Address column in input data")


# Form a list of Addresses for geocoding:
# Make a big list of all of the addresses to be processed.
addresses = data[address_column_name].tolist()


#------------------ FUNCTION DEFINITIONS ------------------------

def get_census_results(address):
    """
    Get geocode results from US Census Bureau API.

    Note, that in the case of multiple US Census geocode results, this function returns details of the FIRST result.

    @param return_full_response: Boolean to indicate if you'd like to return the full response from google. This
                    is useful if you'd like additional location details for storage or parsing later.
    """
    # Set up your Geocoding url
    geocode_url = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress?address={}&benchmark=Public_AR_Census2010&vintage=Census2010_Census2010&layers=14&format=json".format(address)


   # Ping US Census Bureau for the reuslts:
    results = requests.get(geocode_url)
    # Results will be in JSON format - convert to dict using requests functionality
    results = results.json()

    # if there's no results or an error, return empty results.
    if len(results['result']) == 0:
        output = {
            "formatted_address" : None,
            "latitude": None,
            "longitude": None,
            "postcode": None,
            "state_code" : None,
            "county_code" : None,
            "tract_code" : None
        }
    else:
        answer = results['result']['addressMatches']
        output = {
            "formatted_address" : answer[0].get('matchedAddress'),
            "latitude": answer[0].get('coordinates').get('y'),
            "longitude": answer[0].get('coordinates').get('x'),
            "postcode": answer[0].get('addressComponents').get('zip'),
            "state_code":  answer[0].get('geographies').get('Census Blocks')[0].get('STATE'),
            "county_code": answer[0].get('geographies').get('Census Blocks')[0].get('COUNTY'),
            "tract_code":answer[0].get('geographies').get('Census Blocks')[0].get('TRACT'),

        }

    # Append some other details:
    output['input_address'] = address
    #output['fips_code'] = pd.concat(output['state_code']+output['county_code']+output['tract_code'])


    return output

#------------------ PROCESSING LOOP -----------------------------

# Ensure, before we start, that the internet access is ok
test_result = get_census_results("425 E 61 ST, New York, NY 10065 ",)
if (test_result['tract_code'] != '010602'):
    logger.warning("There was an error when testing the US Census Bureau Geocoder.")
    raise ConnectionError('Problem with test results from US Census Bureau Geocoder - check your data format and internet connection.')

# Create a list to hold results
results = []
# Go through each address in turn
for address in addresses:
    # While the address geocoding is not finished:
    geocoded = False
    while geocoded is not True:
        # Geocode the address with FCC
        try:
            geocode_result = get_census_results(address)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {}".format(address))
            logger.error("Skipping!")
            geocoded = True

        # If we're over the API limit, backoff for a while and try again later.
        if geocode_result['state_code'] == '':
            logger.info("Hit Query Limit! Backing off for a bit.")
            time.sleep(BACKOFF_TIME * 60) # sleep for 30 minutes
            geocoded = False
        else:
            # If we're ok with API use, save the results
            # Note that the results might be empty / non-ok - log this
            if geocode_result['state_code'] != '36':
                logger.warning("Address Not in New York State {}".format(address))
            logger.debug("Geocoded: {}".format(address))
            results.append(geocode_result)
            geocoded = True

    # Print status every 1000 latlong
    if len(results) % 1000 == 0:
      logger.info("~~~~~~~~COMPLETED {} OF {} ADDRESSES~~~~~~~~".format(len(results), len(addresses)))

    # Every 10000 latlong, save progress to file(in case of a failure so you have something!)
    if len(results) % 10000 == 0:
        pd.DataFrame(results).to_csv("{}_bak".format(output_filename))

# All done
logger.info("~~~~~~~~FINISHED GEOCODING ALL LAT-LONG PAIRS~~~~~~~~")
# Write the full results to csv using the pandas library.
pd.DataFrame(results).to_csv(output_filename, encoding='utf8')
