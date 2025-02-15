import requests
from rank.config import config

## calling recall-service "http://localhost:${RECALL_PORT}" config in start.sh
def get_recall(user_id):
    params = {}
    if user_id is not None:
      params['user_id'] = user_id
    res = requests.get(config['recall_endpoint'] + "/recall", params=params)
    return res.json()
