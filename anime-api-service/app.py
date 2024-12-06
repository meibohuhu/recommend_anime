from flask import Flask, jsonify, request
import api.service_client as service_client
from api.anime import get_anime

app = Flask('api-service')

@app.route("/")     ## 猜你喜欢
def get_recommends():
    user_id = request.args.get('user_id', type=int)

    user_id = request.args.get('user_id', type=int)
    rec_anime_ids = service_client.get_anime(user_id)
    res = [get_anime(id) for id in rec_anime_ids]       
    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route("/sim")   ## 找相似
def get_similar_animes():
    anime_id = request.args.get('anime_id', type=int)
    if anime_id is None:
        return 'bad anime id', 400

    sim_anime_ids = service_client.get_similar_anime(anime_id)
    res = [get_anime(id) for id in sim_anime_ids]

    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
