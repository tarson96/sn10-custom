import os
import subprocess
from dotenv import load_dotenv

# Load the environment variables from .env file
load_dotenv()

# Read Reddit credentials from .env file and store them in a dictionary
reddit_credentials = []
def activate_venv(path_to_venv):
    activate_this = f'{path_to_venv}/bin/activate_this.py'
    with open(activate_this) as file_:
        exec(file_.read(), dict(__file__=activate_this))

# replace '/path/to/your/venv' with the actual path to your virtual environment
# activate_venv('/Users/tarunsoni/bt/du-custom/newvenv')


for i in range(12):
    client_id = os.getenv(f'REDDIT_CLIENT_ID_{i}')
    client_secret = os.getenv(f'REDDIT_CLIENT_SECRET_{i}')
    username = os.getenv(f'REDDIT_USERNAME_{i}')
    password = os.getenv(f'REDDIT_PASSWORD_{i}')

    print(client_id)
    if client_id and client_secret and username and password:
        reddit_credentials.append({
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'password': password
        })

# Restart pm2 processes with new Reddit credentials
for i, credentials in enumerate(reddit_credentials):
    pm2_process_index = str(i)
    client_id = credentials['client_id']
    client_secret = credentials['client_secret']
    username = credentials['username']
    password = credentials['password']

    # Set Reddit credentials as environment variables for pm2 process
    env_vars = {
        f'REDDIT_CLIENT_ID': client_id,
        f'REDDIT_CLIENT_SECRET': client_secret,
        f'REDDIT_USERNAME': username,
        f'REDDIT_PASSWORD': password
    }

    # Restart pm2 process with updated environment variables
    print(f"Restarting pm2 process {pm2_process_index} with Reddit credentials:")
    print(f"Client ID: {client_id}")
    print(f"Client Secret: {client_secret}")
    print(f"Username: {username}")
    print(f"Password: {password}")

    try:
        result = subprocess.run(f'/opt/homebrew/bin/node /Users/tarunsoni/.npm-global/bin/pm2 restart {int(pm2_process_index)}', env=env_vars,shell=True)
        print(result)
    except Exception as e:
        print(f"An error occurred while restarting pm2 process {pm2_process_index}: {e}")

    print(f"pm2 process {pm2_process_index} restarted successfully.")
