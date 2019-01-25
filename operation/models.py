from django.db import models

# Create your models here.

class PsBas(models.Model):
    id=models.BigIntegerField(primary_key=True,db_column='ps_id')
    done_code = models.BigIntegerField()
    ps_status=models.IntegerField(default=0)
    fail_reason=models.CharField(max_length=2000)
    busi_code = models.BigIntegerField(default=0)
    ps_type = models.IntegerField(default=0)
    prio_level = models.IntegerField(default=20)
    ps_service_type = models.CharField(max_length=100)
    bill_id = models.CharField(max_length=64)
    sub_bill_id = models.CharField(max_length=64)
    sub_valid_date = models.DateTimeField(auto_now_add=True)
    create_date = models.DateTimeField(auto_now_add=True)
    status_upd_date = models.DateTimeField(auto_now=True)
    end_date = models.DateTimeField()
    action_id = models.BigIntegerField()
    ps_param = models.TextField(max_length=4000)
    op_id = models.BigIntegerField(default=530)
    region_code = models.CharField(max_length=6)
    service_id = models.IntegerField(default=100)
    sub_plan_no = models.BigIntegerField(default=0)
    retry_times = models.IntegerField(default=5)
    notes = models.CharField(max_length=2000)
    fail_log = models.TextField(max_length=4000)

    def __str__(self):
        return 'psId: %d %s %s %d %s' % (self.id, self.bill_id, self.ps_param, self.ps_status, self.fail_reason)

    class Meta:
        abstract = True

    @classmethod
    def create(cls, ps_id, bill_id, sub_bill_id, ps_service_type, action_id, pa_param):
        ps = cls(id=ps_id, bill_id=bill_id, sub_bill_id=sub_bill_id, ps_service_type=ps_service_type, action_id=action_id, pa_param=pa_param)
        return ps


class PsTmpl(models.Model):
    id=models.BigIntegerField(primary_key=True,db_column='ps_id')
    # region_code = models.CharField(max_length=6)
    bill_id = models.CharField(max_length=64)
    sub_bill_id = models.CharField(max_length=64)
    ps_service_type = models.CharField(max_length=100)
    action_id = models.BigIntegerField()
    ps_param = models.TextField(max_length=4000)

    def __str__(self):
        return 'psId: %d %s %s %d %s' % (self.id, self.bill_id, self.sub_bill_id, self.action_id, self.ps_param)

    class Meta:
        abstract = True

    @classmethod
    def create(cls, ps_id, bill_id, sub_bill_id, ps_service_type, action_id, pa_param):
        psTpl = cls(id=ps_id, bill_id=bill_id, sub_bill_id=sub_bill_id, ps_service_type=ps_service_type, action_id=action_id, pa_param=pa_param)
        return psTpl


class ModelFac(object):
    _instance = dict()

    def __new__(cls, tb_name, base_cls=PsBas):
        """        创建类
        :param base_cls: 类名
        :param tb_name: 表名
        :return:
        """
        new_cls_name = "%s_To_%s" % (base_cls.__name__, '_'.join(map(lambda x: x.capitalize(), tb_name.split('_'))))
        # new_cls_name = '_'.join(map(lambda x: x.capitalize(), tb_name.split('_')))

        if new_cls_name not in cls._instance:
            new_meta_cls = base_cls.Meta
            new_meta_cls.db_table = tb_name
            model_cls = type(str(new_cls_name), (base_cls,),
                             {'__tablename__': tb_name, 'Meta': new_meta_cls, '__module__': cls.__module__})
            cls._instance[new_cls_name] = model_cls
        return cls._instance[new_cls_name]


class NumberArea(models.Model):
    start_number = models.BigIntegerField(primary_key=True,db_column='ps_id')
    end_number = models.BigIntegerField(primary_key=True, db_column='ps_id')
    region_code = models.CharField(max_length=6)
    def __str__(self):
        return 'psId: %d %d %s' % (self.start_number, self.end_number, self.region_code)

    class Meta:
        db_table = 'ps_net_number_area'
