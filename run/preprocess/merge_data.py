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

    conf.load('MergeData')
    input_dir = conf.get('input')
    if not input_dir:
        input_dir = os.path.join(conf.get('root_dir'), 'DeAbnormalUser', conf.get('province'), conf.get('month'))

    output_dir = args['output']
    if not output_dir:
        output_dir = os.path.join(conf.get('root_dir'), 'MergeData', conf.get('province'), conf.get('month'))


    cluster = conf.load_to_dict('cluster')

    # Merge Data
    # inputDir outputDir month province abNormalUserDir
    print 'merge data start'
    merge_data_input = input_dir
    merge_data_output = output_dir
    cluster['input_path'] = merge_data_input
    cluster['output_path'] = merge_data_output
    cluster['month'] = conf.get('month')
    cluster['province'] = conf.get('province')

    # params
    merge_data_params = list()
    merge_data_params.append(conf.get('month'))
    merge_data_params.append(conf.get('province'))

    abnormal_user_dir = os.path.join(conf.get('root_dir'), 'User', conf.get('province'), conf.get('month'))
    merge_data_params.append(abnormal_user_dir)
    cluster['params'] = merge_data_params
    cluster['main_class'] = conf.get('main_class')
    cluster['driver'] = conf.get('driver')

    print "params:\n", cluster
    print "in_dir:", input_dir
    print "out_dir:", output_dir

    merge_data_task = runner.SparkJob(**cluster)
    merge_data_task.run()
    print 'merge data end'


if __name__ == '__main__':
    run(option_util.get_option(sys.argv))
