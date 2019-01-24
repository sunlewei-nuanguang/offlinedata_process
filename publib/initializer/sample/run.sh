# 用 loglevel 指定日志等级，用 logtostderr 表明输出到标准错误
python knife/initializer/sample/sample.py --logtostderr --hello=30
python knife/initializer/sample/sample.py --logtostderr --hello=30 --test_bool
DEBUG=False python knife/initializer/sample/sample.py --logtostderr --hello=30
