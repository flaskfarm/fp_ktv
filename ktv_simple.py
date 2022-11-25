# -*- coding: utf-8 -*-
#########################################################
# python
import json
import os
import re
import shutil
import sys
import threading
import time
import traceback
from datetime import datetime

# third-party
import requests
import yaml
# third-party
from flask import jsonify, redirect, render_template, request
# sjva 공용
from framework import (SystemModelSetting, Util, app, celery, db,
                       path_app_root, path_data, scheduler, socketio)
from plugin import LogicModuleBase, default_route_socketio
from sqlalchemy import and_, desc, func, not_, or_
from tool_base import ToolBaseFile, d
from tool_expand import EntityKtv

# 패키지
from .plugin import P

logger = P.logger
package_name = P.package_name
ModelSetting = P.ModelSetting
name = 'simple'

#########################################################
class LogicKtvSimple(LogicModuleBase):
    db_default = {
        f'{name}_db_version' : '1',
        f'{name}_interval' : '30',
        f'{name}_auto_start' : 'False',
        f'{name}_path_source' : '',
        f'{name}_path_target' : '',
        f'{name}_path_error' : '',
        f'{name}_path_config' : os.path.join(path_data, 'db', f"{package_name}_{name}.yaml"),
        f'{name}_task_stop_flag' : 'False',
        f'{name}_dry_task_stop_flag' : 'False',
    }

    def __init__(self, P):
        super(LogicKtvSimple, self).__init__(P, 'setting', scheduler_desc='국내TV 파일처리 - Simple')
        self.name = name
        self.data = {
            'data' : [],
            'is_working' : 'wait'
        }
        default_route_socketio(P, self)

    def process_menu(self, sub, req):
        try:
            arg = P.ModelSetting.to_dict()
            arg['sub'] = self.name
            if sub == 'setting':
                arg['is_include'] = scheduler.is_include(self.get_scheduler_name())
                arg['is_running'] = scheduler.is_running(self.get_scheduler_name())
                arg['path_app_root'] = path_app_root
            return render_template(f'{package_name}_{name}_{sub}.html', arg=arg)
        except Exception as e:
            logger.error(f"Exception:{str(e)}")
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f"{package_name}/{name}/{sub}")

    def process_ajax(self, sub, req):
        try:
            if sub == 'command':
                command = req.form['command']
                ret = {}
                if command == 'refresh':
                    self.refresh_data()
                elif command == 'dry_run_start':
                    def func():
                        self.call_task(is_dry=True)
                    th = threading.Thread(target=func, args=())
                    th.setDaemon(True)
                    th.start()
                    ret = {'ret':'success', 'msg':'곧 실행됩니다.'}
                elif command == 'dry_run_stop':
                    if self.data['is_working'] == 'run':
                        ModelSetting.set(f'{name}_dry_task_stop_flag', 'True')
                        ModelSetting.set(f'{name}_task_stop_flag', 'True')
                        ret = {'ret':'success', 'msg':'잠시 후 중지됩니다.'}
                    else:
                        ret = {'ret':'warning', 'msg':'대기중입니다.'}
                return jsonify(ret)
        except Exception as e: 
            P.logger.error(f"Exception:{str(e)}")
            P.logger.error(traceback.format_exc())
            return jsonify({'ret':'danger', 'msg':str(e)})

    def scheduler_function(self):
        self.call_task()
    
    def call_task(self, is_dry=False):
        config = self.load_basic_config()
        self.data['data'] = []
        self.data['is_working'] = 'run'
        self.refresh_data()
        call_module = name
        if is_dry:
            call_module += '_dry'
        ModelSetting.set(f'{call_module}_task_stop_flag', 'False')
        if app.config['config']['use_celery']:
            result = Task.start.apply_async((config, call_module))
            try:
                ret = result.get(on_message=self.receive_from_task, propagate=True)
            except:
                logger.debug('CELERY on_message not process.. only get() start')
                ret = result.get()
        else:
            Task.start(config, call_module)
        self.data['is_working'] = ret
        self.refresh_data()   

    def plugin_load(self):
        if os.path.exists(ModelSetting.get(f'{name}_path_config')) == False:
            shutil.copyfile(os.path.join(os.path.dirname(__file__), 'file', f'config_{name}.yaml'), ModelSetting.get(f'{name}_path_config'))
        #Task.start()
        #load_yaml()
        pass


    #########################################################
    def load_basic_config(self):
        with open(ModelSetting.get(f'{name}_path_config')) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
        return config

    def refresh_data(self, index=-1):
        if index == -1:
            self.socketio_callback('refresh_all', self.data)
        else:
            self.socketio_callback('refresh_one', self.data['data'][index])
    
    def receive_from_task(self, arg, celery=True):
        try:
            result = None
            if celery:
                if arg['status'] == 'PROGRESS':
                    result = arg['result']
            else:
                result = arg
            if result is not None:
                result['index'] = len(self.data['data'])
                self.data['data'].append(result)
                self.refresh_data(index=result['index'])
        except Exception as e: 
            logger.error(f"Exception:{str(e)}")
            logger.error(traceback.format_exc())


from .task_for_download import Task as DownloadProcessTask


class Task(object):
    @staticmethod
    @celery.task(bind=True)
    def start(self, config, call_module):
        logger.warning(f"Simple Task.start")

        is_dry = True if call_module.find('_dry') != -1 else False
        source = ModelSetting.get(f'{name}_path_source')
        target = ModelSetting.get(f'{name}_path_target')
        error = ModelSetting.get(f'{name}_path_error')

        logger.debug(f"소스 : {source}")
        logger.debug(f"target : {target}")
        logger.debug(f"error : {error}")
        for base, dirs, files in os.walk(source):
            logger.warning("BASE : {base}")
            for idx, original_filename in enumerate(files):
                if ModelSetting.get_bool(f"{call_module}_task_stop_flag"):
                    logger.warning("사용자 중지")
                    return 'stop'
                try:
                    data = {'filename':original_filename, 'foldername':base, 'log':[]}
                    filename = original_filename
                    logger.warning(f"{idx} / {len(files)} : {filename}")
                    filename = DownloadProcessTask.process_pre(config, base, filename, is_dry, data)
                    data['filename_pre'] = filename
                    if filename is None:
                        continue
                    entity = EntityKtv(filename, dirname=base, meta=False, config=config)
                    data['entity'] = entity.data
                    if entity.data['filename']['is_matched']:
                        data['result_folder']  = os.path.join(target, entity.data['filename']['name'])
                        data['result_filename'] = entity.data['filename']['original']
                        if is_dry == False:
                            ToolBaseFile.file_move(os.path.join(base, original_filename), data['result_folder'], data['result_filename'])
                    else:
                        data['result_folder'] = error
                        data['result_filename'] = original_filename
                        if is_dry == False:
                            ToolBaseFile.file_move(os.path.join(base, original_filename), data['result_folder'], data['result_filename'])
                except Exception as e: 
                    P.logger.error(f"Exception:{e}")
                    P.logger.error(traceback.format_exc())
                finally:
                    if app.config['config']['use_celery']:
                        self.update_state(state='PROGRESS', meta=data)
                    else:
                        P.logic.get_module(call_module.replace('_dry', '')).receive_from_task(data, celery=False)
                    
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
        
        logger.error(f"종료")
        return 'wait'
