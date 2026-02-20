import requests
import time
import json
from config import read_config
import random

# Set up the API request
# url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"

# Updated endpoint for Gemini 1.5 Flash
# model_name = "models/gemini-1.5-flash"
url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"



# params = {'key': 'your_api_key'}  # Replace 'your_api_key' with the actual API key
gemini_config = read_config()['GeminiPro']
# API_KEY = gemini_config['API_KEY']
API_KEY = random.choice([gemini_config['API_KEY_RH'], gemini_config['API_KEY_RHA']])

headers = {
    'Content-Type': 'application/json',
    'X-goog-api-key': API_KEY
}


params = {'key': API_KEY}  # Use the actual API key provided

def infer(payload):
    try_count = 1
    retry_delay = 5  # sleep for 5 seconds before retrying

    while try_count < 5:
        try:
            # Call Gemini API
            start_time = time.time()  # Start timing

            response = requests.post(url, json=payload, headers=headers, params=params)
            if response.status_code == 200:
                result = response.json()
                # print(result)
                result_json_str = result['candidates'][0]['content']['parts'][0]['text']
                # print(result_json_str)
                result_json_str = result_json_str.lstrip("```").rstrip("```").replace("json", "").replace("\n", "").replace("\t", "").replace("\r", "")
                # print(result_json_str)
                result_dict = json.loads(result_json_str)
                # print(result_dict)
                end_time = time.time()  # End timing
                print(f"get time: {end_time - start_time} seconds")
                break
            else:
                print(f"API request failed with status code {response.status_code}. Retrying...")
                try_count += 1
        except Exception as e:
            print(f"API request failed with exception {e}. Retrying...")
            time.sleep(retry_delay)
            try_count += 1
            retry_delay *= 2

    return result_dict

