import os
from pybuilder.core import use_plugin, inti

use_plugin('python.core')
use_plugin('python.install_dependencies')
use_plugin('python.distutils')
use_plugin("python.unittest")
use_plugin('python.pycharm')
use_plugin('python.pylint')
default_task = "publish"

@init
def initialize(project):
    
    #Set source directory
    project.set_property('dir_source_main_python', 'etl_process')
    project.set_property('dir_source_unittest_python', 'unittests')
    
    #install dependencies
    project.depends_on_requirements("requirements.txt")
   



