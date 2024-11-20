import requests
import json


# URL of the server endpoint
url = "https://artofproblemsolving.com/m/class/ajax.php"

# Form data to be sent


webscraper = (
    "e545b442-9353-498d-8c83-1f3162fd1106.uYYIc5Zym6p%2Fu829y5IuCUXUWYsqjQ8M6ZTTuIOoA70"
)
me = (
    "0df81ddb-dd00-4bf4-b9ec-234e748516ca.GVbSpMNyedz0JjwtDQc34c%2BrpPyiiYgy5ydpeVkHOfk"
)
lesson_id = 1
while True:
    form_data = {
        "class_id": 3686,
        "lesson[]": lesson_id,
        "display": 1,
        "a": "get_class_homework",
    }
    # Sending the POST request
    response = requests.post(
        url,
        data=form_data,
        cookies={
            "platsessionid": me,
        },
    )
    data = response.json()
    if "Solution" in str(data):
        open("data.json", "w").write(json.dumps(data))
        break
    lesson_id += 1
# problems = data["response"]["problems"]

# problem_data = problems[4]
# solution = problem_data["solution_text"]
# print(
#     list(problem_data.keys()),
#     problem_data["answer"],
#     problem_data["answer_type"],
#     problem_data["alt_answers"],
#     problem_data["available_hints"],
#     problem_data["my_hints_fmt"],
#     problem_data["can_hint"],
#     problem_data["problem_has_solution"],
#     problem_data["solution_text"],
#     problem_data["problem_type"],
#     sep="\n",
# )
