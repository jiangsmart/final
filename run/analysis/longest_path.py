# -*- coding: UTF-8 -*-
import sys
from jobControl import runner
from util import project_dir_manager, conf_parser, assert_message, hdfs_util, option_util
import os


def run(args):
    conf_file = args['conf']
    conf = conf_parser.ConfParser(conf_file)
    conf.load('LongestPath')            # 加载去震荡默认的参数配置模块
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
    cluster = conf.load_to_dict('cluster')
    
    input_dir = args['input']
    if not input_dir:
        input_dir = '/user/tele/trip'

    output_dir = args['output']
    if not output_dir:
        output_dir = os.path.join(conf.get('root_dir'), 'Extraction', province, month, '%sReprestRtFromHmToWk.csv' %month)

    scenicAnalysis_input = list()
    scenicAnalysis_params = list()
    print "Check input (file src is listed as follows)"
    
    #input 1
    LocalStopPoint_input = os.path.join(conf.get('root_dir'), 'Extraction', province, month, '%sLocalStop.csv' %month)
    print "LocalStopPoint:"+LocalStopPoint_input
    scenicAnalysis_input.append(LocalStopPoint_input)
    #input 2
    UserHome_input = os.path.join(conf.get('root_dir'), 'Extraction', province, month, '%sHome.csv' %month)
    print "UserHome:"+UserHome_input
    scenicAnalysis_input.append(UserHome_input)
    #input 3
    UserWork_input = os.path.join(conf.get('root_dir'), 'Extraction', province, month, '%sWork.csv' %month)
    print "UserWork:"+UserWork_input
    scenicAnalysis_input.append(UserWork_input)
    #input 4
    CellData_input = os.path.join(conf.get('root_dir'), 'BasicInfo', 'BaseStation', '%s.csv' %province)
    print "CellData:"+CellData_input
    scenicAnalysis_input.append(CellData_input)
    #判断输入是否存在
    for inputfiles in scenicAnalysis_input:
        if not hdfs_util.exist(inputfiles):
           print >> sys.stderr, 'the input is not existed!'
           sys.exit(-1) 
    #插入Jar包的参数
    scenicAnalysis_params.append(province)
    scenicAnalysis_params.append(month)
    print 'Extract Local Users Representative(Longest) Path From Home to Work start!'
    cluster['input_path'] = input_dir
    cluster['output_path'] = output_dir
    cluster['params'] = scenicAnalysis_params
    cluster['main_class'] = conf.load_to_dict('LongestPath').get('main_class')
    cluster['driver'] = conf.load_to_dict('LongestPath').get('driver')
    ScenicAnalysis_task = runner.SparkJob(**cluster)
    ScenicAnalysis_task.run()
    print 'Extract Local Users Representative(Longest) Path From Home to Work end!'

if __name__=='__main__':
    run(option_util.get_option(sys.argv))
