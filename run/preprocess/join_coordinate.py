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

    conf.load('JoinCoordinate')
    input_dir = conf.get('input')
    if not input_dir:
        input_dir = os.path.join(conf.get('root_dir'), 'SelectField', conf.get('province'), conf.get('month'))

    output_dir = args['output']
    if not output_dir:
        output_dir = os.path.join(conf.get('root_dir'), 'JoinCoordin', conf.get('province'), conf.get('month'))
    cluster = conf.load_to_dict('cluster')

    # Join Coordinate
    # JoinUserLatLontAndProvince inputDir outputDir month province CellListDir TeleUserInfoDir
    print 'join coordinate start'
    join_coordinate_input = input_dir
    join_coordinate_output = output_dir
    cluster['input_path'] = join_coordinate_input
    cluster['output_path'] = join_coordinate_output
    # cluster['month'] = month
    # cluster['province'] = province
    join_coordinate_params = list()
    join_coordinate_params.append(conf.get('month'))
    join_coordinate_params.append(conf.get('province'))
    join_coordinate_params.append(conf.get('cell_info_dir'))
    join_coordinate_params.append(conf.get('tele_usmergeDistinctNormalUserLocer_info_dir'))  # 是否传参
    cluster['params'] = join_coordinate_params
    cluster['main_class'] = conf.get('main_class')
    cluster['driver'] = conf.get('driver')

    print "params:\n", cluster
    print "in_dir:", input_dir
    print "out_dir:", output_dir

    join_coordinate_task = runner.SparkJob(**cluster)
    join_coordinate_task.run()
    print 'join coordinate end'


if __name__ == '__main__':
    run(option_util.get_option(sys.argv))
