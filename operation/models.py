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

    def __str__(self):
        return 'psId: %d %s %s %d %s' % (self.id, self.bill_id, self.ps_param, self.ps_status, self.fail_reason)

    class Meta:
        abstract = True

    @classmethod
    def setDb_table(Class, tableName):
        class Meta:
            db_table = tableName

        attrs = {
            '__module__': Class.__module__,
            'Meta': Meta
        }
        return type(tableName, (Class,), attrs)


class PsTmpl(models.Model):
    id=models.BigIntegerField(primary_key=True,db_column='ps_id')
    region_code = models.CharField(max_length=6)
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
    def setDb_table(Class, tableName):
        class Meta:
            db_table = tableName

        attrs = {
            '__module__': Class.__module__,
            'Meta': Meta
        }
        return type(tableName, (Class,), attrs)

