# -*- coding: UTF-8 -*-
import sys
from jobControl import runner
from util import project_dir_manager, conf_parser, assert_message, hdfs_util, option_util
import os


def run(args):
    conf_file = args['conf']
    conf = conf_parser.ConfParser(conf_file)
    conf.load('DeOscillation')  # 加载去震荡默认的参数配置模块
    month = args['month']
    if not month:
        if conf.has('month'):
            month = conf.get('month')
        else:
            assert False, 'the month is not setted'

    province = args['province']
    if not province:
        if conf.has('province'):
            province = conf.get('province')
        else:
            assert False, 'the province is not setted'

    user_type = ''
    if conf.has('user_type'):
        user_type = conf.get('user_type')
    if args['params']:
        user_type = args['params'][0]  # 若user_type 采用传参的话，必须把user_type放在无参数名的第一位
    if not user_type:
        assert False, 'the user_type is not setted'

    input_dir = args['input']
    if not input_dir:
        input_dir = os.path.join(conf.get('root_dir'), 'DataClean', month)

    output_dir = args['output']
    if not output_dir:
        output_dir = os.path.join(conf.get('root_dir'), 'DeOscillation', month)
    cluster = conf.load_to_dict('cluster')

    # Stable Point
    print 'stable point start'
    if user_type == 'Local':
        stable_input = os.path.join(input_dir, '%sTrue%s.csv' % (month, user_type))
    else:
        stable_input = os.path.join(input_dir, '%sTotal%s.csv' % (month, user_type))
    print stable_input
    stable_output = os.path.join(output_dir, '%sStable%s.csv' % (month, user_type))
    stable_params = list()
    stable_params.append(conf.load_to_dict('StablePoint').get('oscillation.stable.point.time.threshold', '15'))
    cluster['input_path'] = stable_input
    cluster['output_path'] = stable_output
    cluster['params'] = stable_params
    cluster['main_class'] = conf.load_to_dict('StablePoint').get('main_class')
    cluster['driver'] = conf.load_to_dict('StablePoint').get('driver')
    stable_task = runner.SparkJob(**cluster)
    stable_task.run()
    print 'stable point end'

    # Rule1
    print 'rule1 2 3 start'
    rule123_input = stable_output
    rule123_output = os.path.join(output_dir, '%sRule123%s.csv' % (month, user_type))
    rule123_params = list()
    rule123_params.append(conf.load_to_dict('Rule1').get('oscillation.rule1.time.threshold', '2'))
    rule123_params.append(conf.load_to_dict('Rule2').get('oscillation.rule2.time.threshold', '1'))
    rule123_params.append(conf.load_to_dict('Rule2').get('oscillation.rule2.distance.threshold', '10'))
    rule123_params.append(conf.load_to_dict('Rule3').get('oscillation.rule3.speed.threshold', '250'))
    rule123_params.append(conf.load_to_dict('Rule3').get('oscillation.rule3.distance.threshold', '50'))
    cluster['input_path'] = rule123_input
    cluster['output_path'] = rule123_output
    cluster['params'] = rule123_params
    cluster['main_class'] = conf.load_to_dict('Rule1').get('main_class')
    cluster['driver'] = conf.load_to_dict('Rule1').get('driver')
    rule1_task = runner.SparkJob(**cluster)
    rule1_task.run()
    print 'rule123 end'

    # Rule4
    print 'rule4 start'
    rule4_input = rule123_output
    rule4_output = os.path.join(output_dir, '%sRule4%s.csv' % (month, user_type))
    rule4_params = list()
    rule4_params.append(conf.load_to_dict('Rule4').get('oscillation.rule4.time.threshold', '60'))
    rule4_params.append(conf.load_to_dict('Rule4').get('oscillation.rule4.count.threshold', '3'))
    rule4_params.append(conf.load_to_dict('Rule4').get('oscillation.rule4.uniq.count.threshold', '2'))
    cluster['input_path'] = rule4_input
    cluster['output_path'] = rule4_output
    cluster['params'] = rule4_params
    cluster['main_class'] = conf.load_to_dict('Rule4').get('main_class')
    cluster['driver'] = conf.load_to_dict('Rule4').get('driver')
    rule4_task = runner.SparkJob(**cluster)
    rule4_task.run()
    print 'rule4 end'


if __name__ == '__main__':
    run(option_util.get_option(sys.argv))
