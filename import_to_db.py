import csv
import json
import pymongo


def main():
    client = pymongo.MongoClient('localhost', 27017)
    db = client["pokemon"]
    global is_types_imported
    global is_pokemons_imported
    global is_pokemon_species_imported
    global is_pokemon_forms_imported
    global is_moves_imported
    global is_languages_imported
    # is_types_imported = True
    # is_pokemon_species_imported = True
    # is_pokemons_imported = True
    # is_pokemon_forms_imported = True
    # is_moves_imported = True
    # is_type_efficacy_imported = True
    # is_languages_imported = True
    import_types(db)
    import_pokemon_species(db)
    import_pokemons(db)
    import_pokemon_forms(db)
    import_moves(db)
    import_type_efficacy(db)
    import_languages(db)

    lang_ja_id = db["languages"].find_one({"identifier": "ja"})["_id"]

    # # ポケモンの種族を列挙
    # for species in db["pokemon_species"].find():
    #     species_name = get_name_by_lang(
    #         species, "names", "name", lang_ja_id)
    #     print(species["number"], species["identifier"], species_name)
    #     # 同じ種族のポケモンを検索
    #     for pokemon in db["pokemons"].find({"species": species["_id"]}):
    #         pokemon_form_name = get_name_by_lang(
    #             pokemon, "form_names", "name", lang_ja_id)
    #         types = [None, None]
    #         if pokemon["types"][0]:
    #             type_doc = db["types"].find_one({"_id": pokemon["types"][0]})
    #             types[0] = get_name_by_lang(
    #                 type_doc, "names", "name", lang_ja_id)
    #         if pokemon["types"][1]:
    #             type_doc = db["types"].find_one({"_id": pokemon["types"][1]})
    #             types[1] = get_name_by_lang(
    #                 type_doc, "names", "name", lang_ja_id)
    #         print("    ", pokemon["identifier"], pokemon_form_name, types)
    #         # # フォルムを検索
    #         # for form in db["pokemon_forms"].find({"pokemon": pokemon["_id"]}):
    #         #
    #         #     form_name = get_name_by_lang(
    #         #         form, "form_names", "name", lang_ja_id)
    #         #     if not form_name:
    #         #         form_name = species_name
    #         #     print("        ", form["identifier"], form_name)


# ポケモンや技のタイプのデータがすでにインポートされたかどうか
is_types_imported = False
# ポケモンの種族のデータがすでにインポートされたかどうか
is_pokemon_species_imported = False
# ポケモンのデータがすでにインポートされたかどうか
is_pokemons_imported = False
# ポケモンのフォルムのデータがすでにインポートされたかどうか
is_pokemon_forms_imported = False
# 技のデータがすでにインポートされたかどうか
is_moves_imported = False
# タイプの相性のデータがすでにインポートされたかどうか
is_type_efficacy_imported = False
# 言語のデータがすでにインポートされたかどうか
is_languages_imported = False


def import_types(db):
    """ポケモンや技のタイプにインポートします。"""
    # 二重インポート防止
    global is_types_imported
    if is_types_imported:
        return
    # 依存するデータのインポート
    import_languages(db)
    # インポート開始
    print("importing types.")
    type_col = db["types"]
    language_col = db["languages"]
    type_col.remove()
    for item in get_types():
        item["names"] = []
        type_col.insert(item)
    # 名前リストをドキュメントに追加
    for item in get_type_names():
        lang_doc = language_col.find_one({"id": item["local_language_id"]})
        type_col.update({"id": item["type_id"]}, {"$push": {
            "names": {
                "language": lang_doc["_id"],
                "name": item["name"]
            }
        }})
    is_types_imported = True


def import_pokemon_species(db):
    """ポケモンの種族をデータベースにインポートします。"""
    # 二重インポート防止
    global is_pokemon_species_imported
    if is_pokemon_species_imported:
        return
    # 依存するデータのインポート
    import_languages(db)
    # インポート開始
    print("importing pokemon_species.")
    pokemon_species_col = db["pokemon_species"]
    language_col = db["languages"]
    pokemon_species_col.remove()  # コレクションをリセット
    for item in get_pokemon_species():
        pokemon_species_col.insert({
            "id": item["id"],
            "number": item["id"],
            "identifier": item["identifier"],
            "evolves_from_species": None,
            "evolves_to_species": None,
            "gender_rate": item["gender_rate"],
            "capture_rate": item["capture_rate"],
            "base_happiness": item["base_happiness"],
            "has_gender_differences": item["has_gender_differences"],
            "has_gender_differences": item["has_gender_differences"],
            "forms_switchable": item["forms_switchable"],
            "order": item["order"],
        })
    # 進化元と進化先をドキュメントに追加
    for item in get_pokemon_species():
        if item["evolves_from_species_id"]:
            pokemon = pokemon_species_col.find_one({
                "id": item["id"],
            })
            evolves_from_species = pokemon_species_col.find_one({
                "id": item["evolves_from_species_id"],
            })
            pokemon_species_col.update({"_id": pokemon["_id"]}, {
                "$set": {
                    "evolves_from_species": evolves_from_species["_id"]
                }
            })
            pokemon_species_col.update({"_id": evolves_from_species["_id"]}, {
                "$set": {
                    "evolves_to_species": pokemon["_id"]
                }
            })
    # 名前リストをドキュメントに追加
    for item in get_pokemon_species_names():
        lang_doc = language_col.find_one({"id": item["local_language_id"]})
        pokemon_species_col.update({"id": item["pokemon_species_id"]}, {
            "$push": {
                "names": {
                    "language": lang_doc["_id"],
                    "name": item["name"]
                },
                "genuses": {
                    "language": lang_doc["_id"],
                    "genus": item["genus"]
                }
            }
        })
    is_pokemon_species_imported = True


def import_pokemons(db):
    """ポケモンをデータベースにインポートします。"""
    # 二重インポート防止
    global is_pokemons_imported
    if is_pokemons_imported:
        return
    # 依存するデータのインポート
    import_pokemon_species(db)
    import_moves(db)
    # インポート開始
    print("importing pokemons.")
    pokemon_col = db["pokemons"]
    pokemon_species_col = db["pokemon_species"]
    type_col = db["types"]
    move_col = db["moves"]
    pokemon_col.remove()
    for item in get_pokemons():
        species = pokemon_species_col.find_one({"id": item["species_id"]})
        item["types"] = [None, None]
        item["species"] = species["_id"]
        # ポケモン種族の名前リストをポケモンドキュメントにコピー
        pokemon_names = []
        for item2 in species["names"]:
            pokemon_names.append({
                "language": item2["language"],
                "name": item2["name"],
            })
        item["pokemon_names"] = pokemon_names
        pokemon_col.insert(item)
    # ポケモンのタイプをドキュメントに追加
    for item in get_pokemon_types():
        type_doc = type_col.find_one({"id": item["type_id"]})
        pokemon_col.update({"id": item["pokemon_id"]}, {
            "$set": {
                "types." + str(item["slot"] - 1): type_doc["_id"]
            }
        })
    # ポケモンの種族値をドキュメントに追加
    for item in get_pokemon_stats():
        stat_name = ""
        if item["stat_id"] == 1:
            stat_name = "hp"
        elif item["stat_id"] == 2:
            stat_name = "attack"
        elif item["stat_id"] == 3:
            stat_name = "defense"
        elif item["stat_id"] == 4:
            stat_name = "special-attack"
        elif item["stat_id"] == 5:
            stat_name = "special-defense"
        elif item["stat_id"] == 6:
            stat_name = "speed"
        else:
            raise Exception("unkown stat_id " + str(item["stat_id"]) + ".")
        pokemon_col.update({"id": item["pokemon_id"]}, {
            "$set": {
                "base_stat_values." + stat_name: item["base_stat"]
            }
        })
    # ポケモンの技をドキュメントに追加
    print("importing pokemons.moves.")
    count = 0
    for item in get_pokemon_moves():
        if count % 100 == 0:
            print(str(count) + "件完了")
        if item["version_group_id"] != 16:
            continue
        move = move_col.find_one({"id": item["move_id"]})
        pokemon_col.update({"id": item["pokemon_id"]}, {
            "$push": {
                "moves": {
                    "move": move["_id"],
                    "level": item["level"]
                    # "version_group_id": item["version_group_id"]
                    # "pokemon_move_method_id": item["pokemon_move_method_id"]
                    # "order": item["order"]
                }
            }
        })
        count += 1
    is_pokemons_imported = True


def import_pokemon_forms(db):
    """ポケモンのフォルムをデータベースにインポートします。"""
    # 二重インポート防止
    global is_pokemon_forms_imported
    if is_pokemon_forms_imported:
        return
    # 依存するデータのインポート
    import_pokemons(db)
    import_languages(db)
    # インポート開始
    print("importing pokemon_forms.")
    pokemon_forms_col = db["pokemon_forms"]
    pokemons_col = db["pokemons"]
    language_col = db["languages"]
    pokemon_forms_col.remove()  # コレクションをリセット
    for item in get_pokemon_forms():
        pokemon_doc = pokemons_col.find_one({"id": item["pokemon_id"]})
        pokemon_forms_col.insert({
            "id": item["id"],
            "identifier": item["identifier"],
            "form_identifier": item["form_identifier"],
            "pokemon": pokemon_doc["_id"],
            "is_default": item["is_default"],
            "is_battle_only": item["is_battle_only"],
            "is_mega": item["is_mega"],
            "form_order": item["form_order"],
            "order": item["order"],
        })
    # 名前リストをドキュメントに追加
    for item in get_pokemon_form_names():
        lang_doc = language_col.find_one({"id": item["local_language_id"]})
        pokemon_forms_col.update({"id": item["pokemon_form_id"]}, {
            "$push": {
                "form_names": {
                    "language": lang_doc["_id"],
                    "name": item["form_name"]
                },
                "pokemon_names": {
                    "language": lang_doc["_id"],
                    "name": item["pokemon_name"]
                }
            }
        })
    # フォルム名前リストをポケモンドキュメントにコピー
    for pokemon in pokemons_col.find():
        form = pokemon_forms_col.find_one(
            {"identifier": pokemon["identifier"]})
        form_names = []
        if form and ("form_names" in form):
            form_names = form["form_names"]
        pokemons_col.update({"_id": pokemon["_id"]}, {
            "$set": {
                "form_names": form_names
            }
        })
    is_pokemon_forms_imported = True


def import_moves(db):
    """技をデータベースにインポートします。"""
    # 二重インポート防止
    global is_moves_imported
    if is_moves_imported:
        return
    # 依存するデータのインポート
    import_types(db)
    # インポート開始
    print("importing moves.")
    moves_col = db["moves"]
    types_col = db["types"]
    language_col = db["languages"]
    moves_col.remove()  # コレクションをリセット
    for item in get_moves():
        type_doc = types_col.find_one({"id": item["type_id"]})
        damage_class = ""
        if item["damage_class_id"] == 1:
            damage_class = "status"
        elif item["damage_class_id"] == 2:
            damage_class = "physical"
        elif item["damage_class_id"] == 3:
            damage_class = "special"
        moves_col.insert({
            "id": item["id"],
            "identifier": item["identifier"],
            "type": type_doc["_id"],
            "power": item["power"],
            "accuracy": item["accuracy"],
            "pp": item["pp"],
            "priority": item["priority"],
            # "target": item["target"],
            "damage_class": damage_class,
            # "generation": item["generation"],
        })
    for item in get_move_names():
        lang_doc = language_col.find_one({"id": item["local_language_id"]})
        moves_col.update({"id": item["move_id"]}, {
            "$push": {
                "names": {
                    "language": lang_doc["_id"],
                    "name": item["name"]
                },
            }
        })
    is_moves_imported = True


def import_type_efficacy(db):
    """タイプ相性をデータベースにインポートします。"""
    # 二重インポート防止
    global is_type_efficacy_imported
    if is_type_efficacy_imported:
        return
    # 依存するデータのインポート
    import_types(db)
    # インポート開始
    print("importing type_efficacy.")
    type_efficacy_col = db["type_efficacy"]
    type_col = db["types"]
    type_efficacy_col.remove()  # コレクションをリセット
    for item in get_type_efficacy():
        damage_type = type_col.find_one({"id": item["damage_type_id"]})
        target_type = type_col.find_one({"id": item["target_type_id"]})
        type_efficacy_col.insert({
            "damage_type": damage_type["_id"],
            "target_type": target_type["_id"],
            "damage_factor": item["damage_factor"]
        })
    is_type_efficacy_imported = True


def import_languages(db):
    """言語をデータベースにインポートします。"""
    # 二重インポート防止
    global is_languages_imported
    if is_languages_imported:
        return
    # インポート開始
    print("importing languages.")
    language_col = db["languages"]
    language_col.remove()  # コレクションをリセット
    for item in get_languages():
        language_col.insert(item)
    is_languages_imported = True


def open_csv(csv_path):
    with open(csv_path, newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        spamreader.__next__()
        for row in spamreader:
            yield row


def get_types():
    """CSVファイルから、ポケモンや技のタイプのデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/types.csv'
    for row in open_csv(csv_path):
        yield {
            "id": int(row[0]),
            "identifier": row[1],
            # "generation_id": int(row[2]),
            # "damage_class_id": int(row[3]),
        }


def get_type_names():
    """CSVファイルから、ポケモンや技のタイプの名前のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/type_names.csv'
    for row in open_csv(csv_path):
        yield {
            "type_id": int(row[0]),
            "local_language_id": int(row[1]),
            "name": row[2],
        }


def get_pokemon_types():
    """CSVファイルから、ポケモンのタイプのデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/pokemon_types.csv'
    for row in open_csv(csv_path):
        yield {
            "pokemon_id": int(row[0]),
            "type_id": int(row[1]),
            "slot": int(row[2]),
        }


def get_pokemon_stats():
    """CSVファイルから、ポケモンの種族値のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/pokemon_stats.csv'
    for row in open_csv(csv_path):
        yield {
            "pokemon_id": int(row[0]),
            "stat_id": int(row[1]),
            "base_stat": int(row[2]),
            "effort": int(row[3]),
        }


def get_pokemon_moves():
    """CSVファイルから、ポケモンの技のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/pokemon_moves.csv'
    for row in open_csv(csv_path):
        yield {
            "pokemon_id": int(row[0]),
            "version_group_id": int(row[1]),
            "move_id": int(row[2]),
            "pokemon_move_method_id": int(row[3]),
            "level": int(row[4]),
            "order": int(row[5]) if row[5] != "" else None,
        }


def get_pokemon_species():
    """CSVファイルから、ポケモンの種族のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/pokemon_species.csv'
    for row in open_csv(csv_path):
        yield {
            "id": int(row[0]),
            "identifier": row[1],
            "generation_id": int(row[2]),
            "evolves_from_species_id": int(row[3]) if row[3] != "" else None,
            "evolution_chain_id": int(row[4]),
            "color_id": int(row[5]),
            "shape_id": int(row[6]),
            "habitat_id": int(row[7]) if row[7] != "" else None,
            "gender_rate": int(row[8]),
            "capture_rate": int(row[9]),
            "base_happiness": int(row[10]),
            "is_baby": int(row[11]),
            "hatch_counter": int(row[12]),
            "has_gender_differences": int(row[13]),
            "growth_rate_id": int(row[14]),
            "forms_switchable": int(row[15]) == 1,
            "order": int(row[16]),
            "conquest_order": int(row[17]) if row[17] != "" else None,
        }


def get_pokemon_species_names():
    """CSVファイルから、ポケモンの種族の名前のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/pokemon_species_names.csv'
    for row in open_csv(csv_path):
        yield {
            "pokemon_species_id": int(row[0]),
            "local_language_id": int(row[1]),
            "name": row[2],
            "genus": row[3],
        }


def get_pokemons():
    """CSVファイルから、ポケモンのデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/pokemon.csv'
    for row in open_csv(csv_path):
        yield {
            "id": int(row[0]),
            "identifier": row[1],
            "species_id": int(row[2]),
            "order": int(row[6]),
        }


def get_pokemon_forms():
    """CSVファイルから、ポケモンのフォルムのデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/pokemon_forms.csv'
    for row in open_csv(csv_path):
        yield {
            "id": int(row[0]),
            "identifier": row[1],
            "form_identifier": row[2],
            "pokemon_id": int(row[3]),
            "introduced_in_version_group_id": int(row[4]),
            "is_default": int(row[5]) == 1,
            "is_battle_only": int(row[6]) == 1,
            "is_mega": int(row[7]) == 1,
            "form_order": int(row[8]),
            "order": int(row[9]),
        }


def get_pokemon_form_names():
    """CSVファイルから、ポケモンのフォルムの名前のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/pokemon_form_names.csv'
    for row in open_csv(csv_path):
        yield {
            "pokemon_form_id": int(row[0]),
            "local_language_id": int(row[1]),
            "form_name": row[2],
            "pokemon_name": row[3],
        }
    csv_path = 'additional_pokemon_form_names.csv'
    for row in open_csv(csv_path):
        yield {
            "pokemon_form_id": int(row[0]),
            "local_language_id": int(row[1]),
            "form_name": row[2],
            "pokemon_name": row[3],
        }


def get_moves():
    """CSVファイルから、ポケモンの技のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/moves.csv'
    for row in open_csv(csv_path):
        yield {
            "id": int(row[0]),
            "identifier": row[1],
            "generation_id": int(row[2]),
            "type_id": int(row[3]),
            "power": int(row[4]) if row[4] != "" else None,
            "pp": int(row[5]) if row[5] != "" else None,
            "accuracy": int(row[6]) if row[6] != "" else None,
            "priority": int(row[7]),
            "target_id": int(row[8]),
            "damage_class_id": int(row[9]),
            "effect_id": int(row[10]),
            "effect_chance": int(row[11]) if row[11] != "" else None,
            "contest_type_id": int(row[12]) if row[12] != "" else None,
            "contest_effect_id": int(row[13]) if row[13] != "" else None,
            "super_contest_effect_id": int(row[14]) if row[14] != "" else None,
        }


def get_move_names():
    """CSVファイルから、ポケモンの技の名前のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/move_names.csv'
    for row in open_csv(csv_path):
        yield {
            "move_id": int(row[0]),
            "local_language_id": int(row[1]),
            "name": row[2]
        }


def get_type_efficacy():
    """CSVファイルから、タイプ相性のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/type_efficacy.csv'
    for row in open_csv(csv_path):
        yield {
            "damage_type_id": int(row[0]),
            "target_type_id": int(row[1]),
            "damage_factor": int(row[2]) / 100,
        }



def get_languages():
    """CSVファイルから、言語のデータを取得します。"""
    csv_path = 'pokeapi/data/v2/csv/languages.csv'
    for row in open_csv(csv_path):
        yield {
            "id": int(row[0]),
            "iso639": row[1],
            "iso3166": row[2],
            "identifier": row[3],
            "official": int(row[4]),
            "order": int(row[5]),
        }


def get_name_by_lang(doc, field_name1, field_name2, lang):
    if field_name1 not in doc:
        return None
    names = doc[field_name1]
    items = list(filter((lambda item: item["language"] == lang), names))
    if len(items) == 1:
        return items[0][field_name2]
    return None

main()
