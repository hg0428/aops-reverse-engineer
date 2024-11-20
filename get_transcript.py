import requests

# URL of the server endpoint
url = "https://artofproblemsolving.com/m/class/ajax.php"

# Form data to be sent
form_data = {
    "class_id": 3669,
    "a": "get-transcript-for-grid",
    "instance_id": 3000,
}

# Sending the POST request
response = requests.post(
    url,
    data=form_data,
    cookies={
        "platsessionid": "729c3171-a94d-4258-a482-1a5b55550573.Y8yf14a6uZhYq3Lho9XIG1yU%2FgDb81NzWcWHSlxI%2BZI",
    },
)
data = response.json()
print(data)
