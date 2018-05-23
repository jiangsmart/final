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

    conf.load('DeAbnormalUser')
    input_dir = conf.get('input')
    if not input_dir:
        input_dir = os.path.join(conf.get('root_dir'), 'SplitbyProv', conf.get('province'), conf.get('month'))

    output_dir = args['output']
    if not output_dir:
        output_dir = os.path.join(conf.get('root_dir'), 'DeAbnormalUser', conf.get('province'), conf.get('month'))

    cluster = conf.load_to_dict('cluster')

    ## DeAbnormal User
    print 'deAbnormal user start'
    de_abnormal_user_input = input_dir
    print de_abnormal_user_input
    de_abnormal_user_output = output_dir
    cluster['input_path'] = de_abnormal_user_input
    cluster['output_path'] = de_abnormal_user_output
    cluster['month'] = conf.get('month')
    cluster['province'] = conf.get('province')
    de_abnormal_user_params = list()
    normal_user_dir = os.path.join(conf.get('root_dir'), 'User', conf.get('province'), conf.get('month'))
    abnormal_user_dir = os.path.join(conf.get('root_dir'), 'User', conf.get('province'), conf.get('month'))
    de_abnormal_user_params.append(conf.get('month'))
    de_abnormal_user_params.append(conf.get('province'))
    de_abnormal_user_params.append(normal_user_dir)
    de_abnormal_user_params.append(abnormal_user_dir)
    de_abnormal_user_params.append(conf.get('cc_threshold'))  # 是否传参
    de_abnormal_user_params.append(conf.get('sm_threshold'))  # 是否传参
    de_abnormal_user_params.append(conf.get('mm_threshold'))  # 是否传参
    cluster['params'] = de_abnormal_user_params
    cluster['main_class'] = conf.get('main_class')
    cluster['driver'] = conf.get('driver')

    print "params:\n", cluster
    print "in_dir:", input_dir
    print "out_dir:", output_dir

    de_abnormal_user_task = runner.SparkJob(**cluster)
    de_abnormal_user_task.run()
    print 'de abnormal user end'


if __name__ == '__main__':
    run(option_util.get_option(sys.argv))
