from geopy.geocoders import Nominatim

def get_country_code(lat, lng):
    geolocator = Nominatim(user_agent="name_of_your_app")
    location = geolocator.reverse((lat, lng), exactly_one=True)
    # address = location.address
    # address = address.split(",")[-1].strip()
    # print(type(location))
    # print(type(location.raw))
    # print(location.raw.keys())
    country_code = location.raw["address"]["country_code"]
    return country_code

lat, lng = 40.7128, -74.0060  # Example: New York
# lat, lng = 30, 110  # Example: cn
country_code = get_country_code(lat, lng)
print(country_code)  # Output: USA
