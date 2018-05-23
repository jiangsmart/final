# -*- coding: utf-8 -*-
import sys
from util import project_dir_manager, conf_parser, option_util
from jobControl import runner
import subprocess
import os


def run(args):
    conf_file = args['conf']
    print "---------", conf_file
    conf = conf_parser.ConfParser(conf_file)

    conf.load('SplitUserbyProvince')
    input_dir = conf.get('input')
    if not input_dir:
        input_dir = os.path.join(conf.get('root_dir'), 'JoinCoordin', conf.get('province'), conf.get('month'))

    output_dir = args['output']
    if not output_dir:
        output_dir = os.path.join(conf.get('root_dir'), 'SplitbyProv', conf.get('province'), conf.get('month'))

    cluster = conf.load_to_dict('cluster')

    # Split User By Province
    # inputDir outputDir month(201512) state provinceMap
    print 'split user by province start'
    split_user_by_province_input = input_dir
    split_user_by_province_output = output_dir
    cluster['input_path'] = split_user_by_province_input
    cluster['output_path'] = split_user_by_province_output
    cluster['month'] = conf.get('month')
    cluster['province'] = conf.get('province')

    # params
    split_user_by_province_params = list()
    split_user_by_province_params.append(conf.get('month'))

    # 以下两个参数待修正,改为可调
    split_user_by_province_params.append('海南')
    split_user_by_province_params.append('/user/tele/trip/BasicInfo/provinceMap')

    cluster['params'] = split_user_by_province_params

    cluster['main_class'] = conf.get('main_class')
    cluster['driver'] = conf.get('driver')

    print "params:\n", cluster
    print "in_dir:", input_dir
    print "out_dir:", output_dir

    split_user_by_province_task = runner.SparkJob(**cluster)
    split_user_by_province_task.run()
    print 'split user by province end'


if __name__ == '__main__':
    run(option_util.get_option(sys.argv))
