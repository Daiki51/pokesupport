import sys
import math
import json
import argparse
import pymongo
from colorama import Fore, Back, Style

# ガルーラデータ
kangaskhan_data = {
    "hp": 147,
    "attack": 100,
    "defense": 54,
    "special-attack": 100,
    "special-defense": 156,
    "base-speed": 100,
}

# さっさと倒したほうがいいポケモン
# ツボツボ
# エテボース
# エーフィ
# ブラッキー
# バリヤード

# ゲンガーに交代
# ブリガロン

# 厄介な技
# トリックルーム
# でんじは
# おにび

# 苦手だけど一発で倒せるポケモン
# チャーレム
# サーナイト
# フーディン (ふいうち)
# ブロスター


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', type=str, default=None)
    args = parser.parse_args()

    client = pymongo.MongoClient('localhost', 27017)
    db = client["pokemon"]
    pokemon_name = args.target or "レジロック"
    pokemon = search_pokemon_by_name(db, pokemon_name)
    # print(pokemon)
    print("味方のポケモン:", "ガルーラ")
    print("相手のポケモン:", pokemon_name)
    print("-------------------")
    step1(db, pokemon)
    # print(get_stat_value("hp", 50, 105, 0, 31))
    # print(get_stat_value("a", 50, 95, 0, 31))


def step1(db, pokemon):
    lang_ja_id = db["languages"].find_one({"identifier": "ja"})["_id"]
    if pokemon["base_stat_values"]["speed"] >= kangaskhan_data["base-speed"]:
        print(Fore.RED + "相手ポケモンが先制の可能性があります" + Style.RESET_ALL)
        print("-------------------")
    # 相手の技の中から危険なものを検索
    fighting_type = db["types"].find_one({"identifier": "fighting"})
    exclude_vigilance_moves = [
        # db["moves"].find_one({"names.name": "きあいパンチ"})["_id"],
    ]
    vigilance_moves = []
    for item in pokemon["moves"]:
        move = db["moves"].find_one({"_id": item["move"]})
        if move["_id"] in exclude_vigilance_moves:
            continue
        power = move["power"]
        if not power:
            continue
        if move["type"] == fighting_type["_id"]:
            power *= 2
        if move["type"] in pokemon["types"]:
            power *= 1.5
        if move["damage_class"] == "physical":
            power *= pokemon["base_stat_values"]["attack"]
        elif move["damage_class"] == "special":
            power *= pokemon["base_stat_values"]["special-attack"]
        power /= 100
        # 情報: このpowerが320を超えるとガルーラが倒れる
        if power > 320 * (3 / 10):
            if len(list(filter(
                (lambda item: item["move"]["_id"] == move["_id"]),
                vigilance_moves))) == 0:
                vigilance_moves.append({
                    "move": move,
                    "power": power
                })
    vigilance_moves.sort(key=lambda item: item["power"], reverse=True)
    print("相手の威力の高い技")
    for i in range(len(vigilance_moves)):
        item = vigilance_moves[i]
        move = item["move"]
        power = item["power"]
        move_name = get_name_by_lang(move, "names", "name", lang_ja_id)
        if power > 320 * (7 / 10):
            sys.stdout.write(Fore.RED)
        elif power > 320 * (5 / 10):
            sys.stdout.write(Fore.YELLOW)
        print(str(i + 1) + "位:", move_name, "威力(補正):", int(power), "命中:", move["accuracy"])
        sys.stdout.write(Style.RESET_ALL)
    print("-------------------")
    kangaskhan_moves = [
        db["moves"].find_one({"names.name": "グロウパンチ"}),
        db["moves"].find_one({"names.name": "おんがえし"}),
        db["moves"].find_one({"names.name": "じしん"}),
        db["moves"].find_one({"names.name": "ふいうち"}),
    ]
    normal_type = db["types"].find_one({"names.name": "ノーマル"})
    self_moves = []
    for move in kangaskhan_moves:
        power = move["power"]
        # 技「おんがえし」は威力MAX
        if move == kangaskhan_moves[1]:
            power = 102
        if move["type"] == normal_type["_id"]:
            power *= 1.5

        damage_factor1 = db["type_efficacy"].find_one({
                "damage_type": move["type"],
                "target_type": pokemon["types"][0],
            })["damage_factor"]
        damage_factor2 = 1
        if pokemon["types"][1]:
            damage_factor2 = db["type_efficacy"].find_one({
                    "damage_type": move["type"],
                    "target_type": pokemon["types"][1],
                })["damage_factor"]
        power *= damage_factor1 * damage_factor2
        self_moves.append({
            "move": move,
            "power": power
        })
    self_moves.sort(key=lambda item: item["power"], reverse=True)
    print("ガルーラの有効な技")
    for i in range(len(self_moves)):
        item = self_moves[i]
        move = item["move"]
        power = item["power"]
        move_name = get_name_by_lang(move, "names", "name", lang_ja_id)
        print(str(i + 1) + "位:", move_name, "威力(補正):", int(power))


def search_pokemon_by_name(db, name):
    lang_ja_id = db["languages"].find_one({"identifier": "ja"})["_id"]
    pokemon = db["pokemons"].find_one({"form_names.name": name})
    if pokemon:
        return pokemon
    return db["pokemons"].find_one({"pokemon_names.name": name})


def get_name_by_lang(doc, field_name1, field_name2, lang):
    if field_name1 not in doc:
        return None
    names = doc[field_name1]
    items = list(filter((lambda item: item["language"] == lang), names))
    if len(items) == 1:
        return items[0][field_name2]
    return None

# 個体値を計算
def get_stat_value(stat, level, base_stat_value, effort_value, individual_value, natures=None, natures_effects=None):
    if natures and natures_effects:
        raise Exception("")
    if natures:
        natures_effects = natures["effects"]
    elif not natures_effects:
        natures_effects = 1.0
    tmp = math.floor(effort_value / 4)
    tmp = base_stat_value * 2 + individual_value + tmp
    tmp = math.floor(tmp * level / 100)
    if stat == "hp":
        return tmp + level + 10
    else:
        return tmp + 5


main()
