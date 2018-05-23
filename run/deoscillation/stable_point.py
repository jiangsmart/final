# -*- coding: UTF-8 -*-
import sys
from jobControl import runner
from util import project_dir_manager, conf_parser, assert_message, hdfs_util, option_util


def run(args):
    conf_file = args[1]
    conf = conf_parser.ConfParser(conf_file)
    conf.load('DeOscillation')  # 加载去震荡默认的参数配置模块
    # Stable Point
    print 'stable point start'
    stable_input = args[2]  # os.path.join(input_dir,'%sTotal%s.csv' % (month, user_type))
    print stable_input
    stable_output = args[3]  # os.path.join(output_dir, '%sStable%s.csv' % (month, user_type))
    stable_params = list()
    stable_params.append(conf.load_to_dict('StablePoint').get('oscillation.stable.point.time.threshold', '15'))
    cluster = conf.load_to_dict('cluster')
    cluster['input_path'] = stable_input
    cluster['output_path'] = stable_output
    cluster['params'] = stable_params
    cluster['main_class'] = conf.load_to_dict('StablePoint').get('main_class')
    cluster['driver'] = conf.load_to_dict('StablePoint').get('driver')
    stable_task = runner.SparkJob(**cluster)
    stable_task.run()
    print 'stable point end'


if __name__ == '__main__':
    run(sys.argv)
