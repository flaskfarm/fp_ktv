import json
import os
import re
import traceback

import requests
from support import SupportDiscord, SupportFile
from tool import EntityKtv, ToolNotify

from .model import ModelFPKtvItem
from .setup import F, P


class Task(object):
    @staticmethod
    @F.celery.task(bind=True)
    def start(self, configs, call_module):
        P.logger.warning(f"Task.start : {call_module}")

        is_dry = True if call_module.find('_dry') != -1 else False
        for config in configs:
            source = config['소스 폴더']
            target = config['타겟 폴더']
            error = config['에러 폴더']

            for base, dirs, files in os.walk(source):
                for idx, original_filename in enumerate(files):
                    #if idx>0:return
                    if P.ModelSetting.get_bool(f"{call_module}_task_stop_flag"):
                        P.logger.warning("사용자 중지")
                        return 'stop'
                    try:
                        db_item = ModelFPKtvItem(call_module, original_filename, base, is_dry)
                        
                        filename = original_filename
                        #logger.warning(f"{idx} / {len(files)} : {filename}")
                        filename = Task.process_pre(config, db_item, is_dry)
                        if filename is None:
                            continue
                        db_item.filename_pre = filename
                        entity = EntityKtv(filename, dirname=base, meta=True, config=config)
                        # 파일 너무 커짐
                        #db_item.entity_data = entity.data
                        db_item.meta_find = entity.data['meta']['find']

                        if entity.data['filename']['is_matched']:
                            if entity.data['meta']['find']:
                                Task.move_file(config, entity, db_item, target, is_dry)
                            else:
                                if entity.data['process_info']['status'] == 'ftv':
                                    db_item.status = "MOVE_BY_FTV"
                                    db_item.result_folder = os.path.join(config['경로 설정']['ftv'].format(error=error), f"{entity.data['process_info']['ftv_title']} ({entity.data['process_info']['ftv_year']})", f"Season {entity.data['filename']['sno']}")
                                else:
                                    prefer = None
                                    if config.get('메타 검색 실패시 타겟 폴더 탐색', False):
                                        prefer = Task.get_prefer_folder_nometa(config, entity.data['filename']['name'])
                                    if prefer != None:
                                        db_item.status = "MOVE_BY_NOMETA_BUT_LIBRARY"
                                        db_item.result_folder = prefer
                                    else:   
                                        db_item.status = "MOVE_BY_NOMETA"
                                        db_item.result_folder = config['경로 설정']['no_meta'].format(error=error)
                                        if config.get('메타 검색 실패시 방송별 폴더 생성', False):
                                            db_item.result_folder  = os.path.join(config['경로 설정']['no_meta'].format(error=error), entity.data['filename']['name'])
                                if is_dry == False:
                                    SupportFile.file_move(os.path.join(base, original_filename), db_item.result_folder, db_item.result_filename)
                        else:
                            db_item.status = "MOVE_BY_NOTV"
                            db_item.result_folder = config['경로 설정']['no_tv'].format(error=error)
                            if is_dry == False:
                                SupportFile.file_move(os.path.join(base, original_filename), db_item.result_folder, db_item.result_filename)
                        
                        if config.get('PLEX_MATE_SCAN') != None:
                            for plex_info in config.get('PLEX_MATE_SCAN'):
                                url = f"{plex_info['URL']}/plex_mate/api/scan/do_scan"
                                P.logger.info(f"PLEX_MATE : {url}")
                                plex_target = os.path.join(db_item.result_folder, db_item.result_filename)
                                for rule in plex_info.get('경로변경', []):
                                    plex_target = plex_target.replace(rule['소스'], rule['타겟'])
                                
                                if plex_target[0] == '/':
                                    plex_target = plex_target.replace('\\', '/')
                                else:
                                    plex_target = plex_target.replace('/', '\\')
                                data = {
                                    'callback_id': f"{P.package_name}_basic_{db_item.id}",
                                    'target': plex_target,
                                    'apikey': F.SystemModelSetting.get('apikey'),
                                    'mode': 'ADD',
                                }
                                res = requests.post(url, data=data)
                                #P.logger.info(res)
                                data = res.json()
                                P.logger.info(f"PLEX SCAN 요청 : {url} {data}")
                        
                        # 2024-05-30 디스코드 봇. GDS용 incoming 파일처리. C1
                        if P.ModelSetting.get_bool("basic_is_gds_bot"):
                            if '/mnt/VOD1/파일처리/방송중' in db_item.result_folder:
                                bot = {
                                    't1': 'gds_tool',
                                    't2': 'fp',
                                    't3': 'vod',
                                    'data': {
                                        'of': original_filename,
                                        'st': db_item.status,
                                        'r_fold': db_item.result_folder.replace('/mnt/VOD1/파일처리/방송중', '/ROOT/GDRIVE/VIDEO/방송중'),
                                        'r_file': db_item.result_filename,
                                        'meta': db_item.meta_find,
                                        'poster': entity.data['meta'].get('poster'),
                                    }
                                }
                                SupportDiscord.send_discord_bot_message(json.dumps(bot), "https://discord.com/api/webhooks/1043387235891949599/A4N2PHfKwA6n-5wXfWdPyoSQPXT6wpj1HEmAMEam6v1JnFxd-PyHt-J7pOEkBEMu0Ld1")
                        
                        if P.ModelSetting.get_bool("basic_use_notify"):
                            msg = f"파일: {original_filename}\n최종폴더: {db_item.result_folder}\n최종파일: {db_item.result_filename}"
                            ToolNotify.send_message(msg, message_id="fp_ktv_basic", image_url=entity.data['meta'].get('poster'))

                    except Exception as e:    
                        P.logger.error(f"Exception:{e}")
                        P.logger.error(traceback.format_exc())
                    finally:
                        if db_item != None:
                            db_item.save()
                        if F.config['use_celery']:
                            self.update_state(state='PROGRESS', meta=db_item.as_dict())
                        else:
                            P.logic.get_module(call_module.replace('_dry', '')).receive_from_task(db_item.as_dict(), celery=False)
                        #return 'wait'
                      
                if base != source and len(os.listdir(base)) == 0 :
                    try:
                        if is_dry == False:
                            os.rmdir(base)
                    except Exception as e: 
                        P.logger.error(f"Exception:{e}")
                        P.logger.error(traceback.format_exc())
            for base, dirs, files in os.walk(source):
                if base != source and len(dirs) == 0 and len(files) == 0:
                    try:
                        if is_dry == False:
                            os.rmdir(base)
                    except Exception as e: 
                        P.logger.error(f"Exception:{e}")
                        P.logger.error(traceback.format_exc())
            

        P.logger.debug(f"task {call_module} 종료")
        return 'wait'


    def process_pre(config, db_item, is_dry):
        filename = db_item.filename_original
        if '전처리' not in config:
            return filename

        for key, value in config['전처리'].items():
            if key == '변환':
                if value is None:
                    continue
                for rule in value:
                    try:
                        filename = re.sub(rule['source'], rule['target'], filename).strip()
                    except Exception as e: 
                            P.logger.error(f"Exception:{e}")
                            P.logger.error(traceback.format_exc())

            elif key == '삭제':
                if value is None:
                    continue
                for regex in value:
                    try:
                        if re.search(regex, filename):
                            try:
                                db_item.status = 'REMOVE_BY_PRE'
                                if is_dry == False:
                                    os.remove(os.path.join(db_item.foldername, db_item.filename_original))
                            except Exception as e: 
                                P.logger.error(f"Exception:{e}")
                                P.logger.error(traceback.format_exc())
                            finally:
                                return
                    except Exception as e: 
                            P.logger.error(f"Exception:{e}")
                            P.logger.error(traceback.format_exc())
            
            elif key == '이동':
                if value is None:
                    continue
                for target, regex_list in value.items():
                    for regex in regex_list:
                        try:
                            if re.search(regex, filename):
                                if target[0] == '/' or target[1] == ':': # 절대경로
                                    target_folder = target
                                else:
                                    if target in config['경로 설정']:
                                        target_folder = config['경로 설정'][target].format(error=config['에러 폴더'])
                                    else:
                                        target_folder = os.path.join(config['에러 폴더'], target)
                                db_item.result_folder = target
                                db_item.result_filename = db_item.filename_original
                                db_item.status = "MOVE_BY_PRE"
                                if is_dry == False:
                                    SupportFile.file_move(os.path.join(db_item.foldername, db_item.filename_original), target_folder, db_item.result_filename)
                                return
                        except Exception as e: 
                                P.logger.error(f"Exception:{e}")
                                P.logger.error(traceback.format_exc())
        return filename




    def move_file(config, entity, db_item, target_folder, is_dry):
        source_path = os.path.join(db_item.foldername, db_item.filename_original)
        if True:
            year_tmp = entity.data['meta']['info']['year']
            if year_tmp == 0 or year_tmp == '0':
                year_tmp = ''
            genre = entity.data['meta']['info']['genre'][0].split('/')[0]
            if entity.data['meta']['info']['code'][1] == 'D':
                genre = config['메타 사이트별 장르 접두사']['daum'] + ' ' + genre
            elif entity.data['meta']['info']['code'][1] == 'W':
                genre = config['메타 사이트별 장르 접두사']['wavve'] + ' ' + genre
            elif entity.data['meta']['info']['code'][1] == 'V':
                genre = config['메타 사이트별 장르 접두사']['tving'] + ' ' + genre
            genre = genre.strip()
            genre = config['장르 변경 규칙'].get(genre, genre)

            program_folder = config['타겟 폴더 구조'].format(
                title=SupportFile.text_for_filename(entity.data['meta']['info']['title']), 
                year=year_tmp,
                studio=entity.data['meta']['info']['studio'],
                genre=genre,
                release=entity.data['filename']['release'],
            )
            tmps = program_folder.replace('(1900)', '').replace('()', '').replace('[]', '').strip()
            tmps = re.sub("\s{2,}", ' ', tmps) 
            tmps = re.sub("/{2,}", '/', tmps) 
            tmps = tmps.split('/')
            program_folder = os.path.join(target_folder, *tmps)
            program_folder = Task.get_prefer_folder(config, entity, program_folder)

            program_folder = Task.change_target_folder(config, program_folder)

            
            target_filename = entity.get_newfilename()
            if target_filename is not None:
                db_item.result_folder = program_folder
                db_item.result_filename = target_filename
                db_item.status = "MOVE_BY_META"
                if is_dry == False:
                    SupportFile.file_move(source_path, program_folder, target_filename)
            else:
                P.logger.error(f"타겟 파일 None")



    def __check_target_folder(config):
        if 'target_folder_list' not in config:
            config['target_folder_list'] = []
            if config['타겟 폴더 탐색 사용'].startswith("특정폴더"):
                for tmp in config['특정폴더']:
                    folderpath = os.path.join(config['타겟 폴더'], tmp)
                    for name in os.listdir(folderpath):
                        titlepath = os.path.join(folderpath, name)
                        if os.path.isdir(titlepath):
                            P.logger.debug(f"타겟폴더1: {titlepath}")
                            config['target_folder_list'].append(titlepath)
            else:
                for genre in os.listdir(config['타겟 폴더']):
                    genre_path = os.path.join(config['타겟 폴더'], genre)
                    if os.path.isdir(genre_path) == False:
                        continue
                    for title in os.listdir(genre_path):
                        title_path = os.path.join(genre_path, title)
                        if os.path.isdir(title_path) == False:
                            continue
                        P.logger.debug(f"타겟폴더2: {title_path}")   

                        config['target_folder_list'].append(title_path)


    def get_prefer_folder(config, entity, program_folder):
        if config['타겟 폴더 탐색 사용'] == '미사용':
            return program_folder
        
        compare_folder_name = os.path.split(program_folder)[-1]
        Task.__check_target_folder(config)

        for _dir in config['target_folder_list']:
            folder_name = os.path.split(_dir)[-1]
            if config['타겟 폴더 탐색 사용'].endswith('방송제목포함'):
                if folder_name.find(entity.data['meta']['info']['title']) != -1:
                    return _dir
            elif config['타겟 폴더 탐색 사용'].endswith('완전일치'):
                if compare_folder_name == folder_name:
                    return _dir
        return program_folder 


    def get_prefer_folder_nometa(config, program_name):
        Task.__check_target_folder(config)

        for _dir in config['target_folder_list']:
            folder_name = os.path.split(_dir)[-1]
            if folder_name.find(program_name) != -1:
                return _dir
    

    def change_target_folder(config, program_folder):
        rules = config.get('타겟 폴더 변환 규칙')
        rules_except = config.get('타겟 폴더 변환 규칙 예외', [])
        for rule in rules_except:
            if rule in program_folder:
                return program_folder

        if rules:
            for rule in rules:
                program_folder = program_folder.replace(rule['source'], rule['target'])
        return program_folder

