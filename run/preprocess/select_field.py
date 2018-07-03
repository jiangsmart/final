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

    conf.load('SelectFields')
    input_dir = conf.get('input')
    if not input_dir:
        input_dir = os.path.join(conf.get('root_dir'), 'SplitData', conf.get('province'), conf.get('month'))

    output_dir = args['output']
    if not output_dir:
        output_dir = os.path.join(conf.get('root_dir'), 'SelectField', conf.get('province'), conf.get('month'))

    cluster = conf.load_to_dict('cluster')
    # Select Fields
    # inputDir outputDir month province
    print 'select fields start'
    select_fields_input = input_dir
    print select_fields_input
    select_fields_output = output_dir
    cluster['input_path'] = select_fields_input
    cluster['output_path'] = select_fields_output

    # params
    select_fields_params = list()
    select_fields_params.append(conf.get('month'))
    select_fields_params.append(conf.get('province'))

    cluster['params'] = select_fields_params
    cluster['main_class'] = conf.get('main_class')
    cluster['driver'] = conf.get('driver')

    print "params:\n", cluster
    print "in_dir:", input_dir
    print "out_dir:", output_dir

    select_fields_task = runner.SparkJob(**cluster)
    select_fields_task.run()
    print 'select_fields end'


if __name__ == '__main__':
    run(option_util.get_option(sys.argv))
