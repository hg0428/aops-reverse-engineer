import requests

# URL of the server endpoint
url = "https://artofproblemsolving.com/m/ebooks/ajax.php"

# Form data to be sent
# Allowed books: 1, 2, 6, 8
form_data = {
    "book_id": 12,
    "section_id": 3600,
    "user_id": 787033,
    "rerender": 0,
    "a": "load_section",
    "aops_logged_in": "true",
    "aops_user_id": 787033,
    "aops_session_id": "70e2244a5ff881b9a359f312b478ae8b",
}

# Sending the POST request
response = requests.post(
    url,
    data=form_data,
    cookies={
        "optimizelyEndUserId": "oeu1723597582148r0.877051899773388",
        "_gcl_au": "1.1.2107804665.1723597583",
        "_fbp": "fb.1.1723597582836.101205855586311965",
        "_ga": "GA1.1.2071698697.1723597583",
        "_hjSessionUser_774800": "eyJpZCI6IjlhMDEzMzg4LWZlNTQtNWEyZC1iNmJjLWU1ZTc1YjJmN2M1MCIsImNyZWF0ZWQiOjE3MjM1OTc1ODMxMTAsImV4aXN0aW5nIjp0cnVlfQ==",
        "f2": "c0b6a8c0c0d9edca87e61a61c1d299784e7691f6fe2c0f2ffbd674a8f3908cf17422aa998bb0e91ea67437fbda19f68679066d722a8d8ee6f150379b973589c7",
        "platsessionid__expires": "1734116270514",
        "platsessionid": "d33435b7-109b-4f0d-82e2-7e0290707dea.zr4nTvLjziVA1MoqVxykn3jqc9m2FapM652YyFD1oTY",
        "_hjSession_774800": "eyJpZCI6Ijc4YWIzMmEyLTlhODUtNGYyYi04MmVlLTE4NDNkOGJmZWE4OSIsImMiOjE3Mjg5Mzc2ODc1NjIsInMiOjEsInIiOjEsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=",
        "grid_init_time": "1728937704",
        "_uetsid": "311a99308a5e11ef92cd2135bb8a7dbd",
        "_uetvid": "6c0f41f059d911efb4c0659573f8ad0b",
        "_ga_NVWC1BELMR": "GS1.1.1728937687.4.1.1728938659.59.0.0",
    },
)
print(response.json())
# print(response.json()["response"]["book"]["sections"][0]["html"])
# Check if the request was successful
if response.status_code == 200:
    print("success")
else:
    print(f"Status code: {response.status_code}")

## Request:
""":authority:
artofproblemsolving.com
:method:
POST
:path:
/m/class/ajax.php
:scheme:
https
accept:
application/json, text/javascript, */*; q=0.01
accept-encoding:
gzip, deflate, br, zstd
accept-language:
en-US,en;q=0.9
content-length:
164
content-type:
application/x-www-form-urlencoded; charset=UTF-8
cookie:
optimizelyEndUserId=oeu1723597582148r0.877051899773388; _gcl_au=1.1.2107804665.1723597583; _fbp=fb.1.1723597582836.101205855586311965; _ga=GA1.1.2071698697.1723597583; _hjSessionUser_774800=eyJpZCI6IjlhMDEzMzg4LWZlNTQtNWEyZC1iNmJjLWU1ZTc1YjJmN2M1MCIsImNyZWF0ZWQiOjE3MjM1OTc1ODMxMTAsImV4aXN0aW5nIjp0cnVlfQ==; f2=c0b6a8c0c0d9edca87e61a61c1d299784e7691f6fe2c0f2ffbd674a8f3908cf17422aa998bb0e91ea67437fbda19f68679066d722a8d8ee6f150379b973589c7; platsessionid__expires=1734116270514; platsessionid=d33435b7-109b-4f0d-82e2-7e0290707dea.zr4nTvLjziVA1MoqVxykn3jqc9m2FapM652YyFD1oTY; _hjSession_774800=eyJpZCI6Ijc4YWIzMmEyLTlhODUtNGYyYi04MmVlLTE4NDNkOGJmZWE4OSIsImMiOjE3Mjg5Mzc2ODc1NjIsInMiOjEsInIiOjEsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; grid_init_time=1728937704; _uetsid=311a99308a5e11ef92cd2135bb8a7dbd; _uetvid=6c0f41f059d911efb4c0659573f8ad0b; _ga_NVWC1BELMR=GS1.1.1728937687.4.1.1728938659.59.0.0
origin:
https://artofproblemsolving.com
priority:
u=1, i
referer:
https://artofproblemsolving.com/
sec-ch-ua:
"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"
sec-ch-ua-mobile:
?0
sec-ch-ua-platform:
"macOS"
sec-fetch-dest:
empty
sec-fetch-mode:
cors
sec-fetch-site:
same-origin
user-agent:
Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36
x-requested-with:
XMLHttpRequest



"""
