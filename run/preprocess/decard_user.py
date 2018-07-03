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

    conf.load('DeCardUser')
    input_dir = conf.get('input')
    if not input_dir:
        input_dir = os.path.join(conf.get('root_dir'), 'SplitbyProv', conf.get('province'), conf.get('month'))

    output_dir = args['output']
    if not output_dir:
        output_dir = os.path.join(conf.get('root_dir'), 'DecardUser', conf.get('province'), conf.get('month'))

    cluster = conf.load_to_dict('cluster')

    # De Card User
    print 'de card user start'
    de_card_user_input = input_dir
    print de_card_user_input
    de_card_user_output = output_dir
    cluster['input_path'] = de_card_user_input
    cluster['output_path'] = de_card_user_output
    cluster['month'] = conf.get('month')
    cluster['province'] = conf.get('province')
    de_card_user_params = list()
    de_card_user_params.append(conf.get('month'))
    de_card_user_params.append(conf.get('month'))
    true_user_dir = os.path.join(conf.get('root_dir'), 'User', conf.get('province'), conf.get('month'))

    # start_time = args['params'][0]  ##未写
    # end_time = args['params'][1]  ##未写

    start_time = conf.get('start_time')
    end_time = conf.get('end_time')
    day_threshold = conf.get('day_threshold')
    hour_threshold = conf.get('hour_threshold')
    de_card_user_params.append(true_user_dir)
    de_card_user_params.append(start_time)
    de_card_user_params.append(end_time)
    de_card_user_params.append(day_threshold)
    de_card_user_params.append(hour_threshold)
    cluster['params'] = de_card_user_params
    cluster['main_class'] = conf.get('main_class')
    cluster['driver'] = conf.get('driver')

    print "params:\n", cluster
    print "in_dir:", input_dir
    print "out_dir:", output_dir

    de_card_user_task = runner.SparkJob(**cluster)
    de_card_user_task.run()

    print 'de card user end'


if __name__ == '__main__':
    run(option_util.get_option(sys.argv))
