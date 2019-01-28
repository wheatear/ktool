# ktool/operation/sendkt.py
import os
import sys
import time
import datetime
import threading
import queue
import logging
import getopt
import re
import glob
import shutil

pathname = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, pathname)
sys.path.insert(0, os.path.abspath(os.path.join(pathname, '..')))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ktool.settings")

import django
django.setup()

from operation.models import PsIdSeq,PsBas,PsTmpl,ModelFac,NumberArea
import ktool.settings

logger = logging.getLogger('django')
errlog = logging.getLogger('error')


class CmdFile(object):
    def __init__(self, file):
        self.cmdFile = file
        self.netType = None
        self.netCode = None
        self.aCmdTemplates = []
        # self.dTmpl = {}
        self.loadCmd()

    def loadCmd(self):
        logger.info('loading cmd template %s', self.cmdFile)
        fCmd = main.openFile(self.cmdFile, 'r')
        # tplName = os.path.basename(self.cmdFile)
        tmplCmd = None
        i = 0
        for line in fCmd:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                if line[:8] == '#NETTYPE':
                    aLine = line.split()
                    self.netType = aLine[2]
                if line[:8] == '#NETCODE':
                    aCode = line.split()
                    self.netCode = aCode[2]
                continue

            if line == '$END$':
                if self.psEnough(tmplCmd):
                    # tmplCmd.ps_model_name = tplName
                    logger.info(tmplCmd)
                    self.aCmdTemplates.append(tmplCmd)
                    tmplCmd = None
                else:
                    logger.error('tmpl attr not enough.')
                continue
            if line == 'KT_REQUEST':
                i += 1
                tmplCmd = PsTmpl.create(i)
                continue
            aParam = line.split(' ', 1)
            if len(aParam) < 1:
                logger.warn('cmd attr %s no value', line)
                continue
            tmplCmd.__setattr__(aParam[0].lower(), aParam[1])
        fCmd.close()
        # logger.info(self.aCmdTemplates)

    def psEnough(self, tmpl):
        if 'id' not in tmpl.__dict__:
            logger.error('tmpl %s has no attr %s', self.cmdFile, 'id')
            exit(-1)
        if 'bill_id' not in tmpl.__dict__:
            logger.error('tmpl %s has no attr %s', self.cmdFile, 'bill_id')
            exit(-1)
        if 'sub_bill_id' not in tmpl.__dict__:
            logger.error('tmpl %s has no attr %s', self.cmdFile, 'sub_bill_id')
            exit(-1)
        if 'ps_service_type' not in tmpl.__dict__:
            logger.error('tmpl %s has no attr %s', self.cmdFile, 'ps_service_type')
            exit(-1)
        if 'action_id' not in tmpl.__dict__:
            logger.error('tmpl %s has no attr %s', self.cmdFile, 'action_id')
            exit(-1)
        if 'ps_param' not in tmpl.__dict__:
            logger.error('tmpl %s has no attr %s', self.cmdFile, 'ps_param')
            exit(-1)
        return True


class DataFile(object):
    def __init__(self, file):
        self.dataFile = file
        self.lineNum = 0
        self.doneNum = 0
        self.fIn = None
        self.aFields = []
        self.openData()
        self.loadFildName()
        self.lineCount()
        self.rspFile = RspFile('%s.rsp' % file)
        self.rspFile.rspTotal = self.lineNum

    def openData(self):
        if not self.fIn:
            self.fIn = main.openFile(self.dataFile, 'r')

    def loadFildName(self):
        fildName = self.fIn.readline()
        logger.info('field: %s', fildName)
        fildName = fildName.lower()
        self.aFields = fildName.split()

    def lineCount(self):
        for line in self.fIn:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            self.lineNum += 1
        self.fIn.seek()
        self.fIn.readline()

    def next(self):
        for line in self.fIn:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            aPara = line.split()
            dPsData = {}
            for i,fild in enumerate(self.aFields):
                dPsData[fild] = aPara[i]
            self.doneNum += 1
            return dPsData
        return False

    def close(self):
        if self.fIn:
            self.fIn.close()


class RspFile(object):
    def __init__(self, file):
        self.rspFile = file
        self.rspTotal = 0
        self.rspDone = 0
        self.fOut = None

    def openRsp(self):
        self.fOut = main.openFile(self.rspFile, 'w')

    def saveRsp(self, line):
        if not self.fOut:
            self.openRsp()
        self.fOut.write('%s%s', line, os.linesep)
        self.rspDone += 1

    def close(self):
        if self.fOut:
            self.fOut.close()

    def back(self):
        if self.fOut:
            self.fOut.close()
        baseFile = os.path.basename(self.rspFile)
        bkFile = os.path.join(main.dirBack, baseFile)
        outFile = os.path.join(main.dirOut, baseFile)
        shutil.copy(self.rspFile, bkFile)
        os.rename(self.rspFile, outFile)


class PsBuilder_FF(object):
    def __init__(self):
        self.inFile = main.inFile
        self.cmdTpl = main.cmdTpl
        self.cmdRsp = None

        self.aDataFile = []

        # self.aFiles = []
        # self.dFiles = {}
        # self.orderDs = None
        # self.aNetInfo = []
        # self.dNetClient = {}
        # self.respName = '%s.rsp' % os.path.basename(self.main.outFile)
        # self.respFullName = os.path.join(self.main.dirOutput, self.respName)
        self.resp = None
        self.aCmdTemplates = []
        self.dNumberArea = {}

    def findFile(self):
        logger.info('find data files ')
        if self.inFile:
            aFiles = glob.glob(self.inFile)
            if len(aFiles) == 0:
                logger.error('no find data file %s', self.inFile)
                # print('no find data file %s' % self.inFile)
                exit(-1)
            logger.info('find data files: %s', aFiles)
            for fi in aFiles:
                baseFile = os.path.basename(fi)
                wkFile = os.path.join(main.dirWork, baseFile)
                bkFile = os.path.join(main.dirBack, baseFile)
                shutil.copy(fi, wkFile)
                os.rename(fi, bkFile)
                dataFile = DataFile(wkFile)
                if len(self.aCmdTemplates)>0:
                    dataFile.rspFile.rspTotal *= len(self.aCmdTemplates)
                else:
                    logger.error('no cmd template, exit.')
                    exit(-1)
                self.aDataFile.append(dataFile)
                # fileRsp = '%s.rsp' % baseFile
                # self.dFiles[fi] = []
        else:
            logger.info('no data file')
            baseFile = os.path.basename(self.cmdTpl)
            cmdRspFile = os.path.join(main.dirWork, baseFile)
            self.cmdRsp = RspFile(cmdRspFile)
            self.cmdRsp.rspTotal = len(self.aCmdTemplates)

    def loadCmd(self):
        logger.info('loading cmd template %s', self.cmdTpl)
        tmplFile = CmdFile(self.cmdTpl)
        self.aCmdTemplates = tmplFile.aCmdTemplates

    def buildQueue(self):
        self.orderQueue = queue.Queue(1000)
        return self.orderQueue

    def buildKtSender(self):
        sender = KtSender(self)
        return sender

    def buildKtRecver(self):
        recver = KtRecver(self)
        return recver

    # def openRsp(self):
    #     if self.resp: return self.resp
    #     self.resp = self.main.openFile(self.respFullName, 'a')
    #     logger.info('open response file: %s', self.respName)
    #     if self.resp is None:
    #         logger.fatal('Can not open response file %s.', self.respName)
    #         exit(2)
    #     return self.resp
    #
    # def saveResp(self, order):
    #     for rsp in order.aResp:
    #         self.resp.write('%s %s%s' % (order.dParam['BILL_ID'], rsp, os.linesep))

    # def makeOrderFildName(self):
    #     fildName = self.orderDs.readline()
    #     logger.info('field: %s', fildName)
    #     fildName = fildName.upper()
    #     self.aFildName = fildName.split()

    # def makeOrder(self):
    #     orderClassName = '%sOrder' % self.netType
    #     logger.debug('load order %s.', orderClassName)
    #     for line in self.orderDs:
    #         line = line.strip()
    #         logger.debug(line)
    #         if len(line) == 0:
    #             continue
    #         if line[0] == '#':
    #             continue
    #         aParams = line.split()
    #
    #         order = createInstance(self.main.appNameBody, orderClassName)
    #         order.setParaName(self.aFildName)
    #         order.setPara(aParams)
    #         logger.debug('order param: %s', order.dParam)
    #         # netCode = self.aNetInfo[0]['NetCode']
    #         order.net = self.dNetClient[self.netCode]
    #         return order
    #     return None

    def loadNumberArea(self):
        logger.info('load number area from ps_net_number_area')
        numberArea = NumberArea.objects.all()
        for num in numberArea:
            start = num.start_number
            end = num.end_number
            regionCode = num.region_code
            while (start <= end):
                nk = start // 10000
                self.dNumberArea[nk] = regionCode
                start += 10000
        logger.info('loaded %d num', len(self.dNumberArea))


class PsBuilder_TF(PsBuilder_FF):
    def __init__(self):
        super(self.__class__, self).__init__()
        self.cmdTab = main.cmdTpl

    def loadCmd(self):
        logger.info('load cmd template.')
        NewTmpl = ModelFac(self.cmdTab, PsTmpl)
        # para = None
        if main.tmplId:
            self.aCmdTemplates = NewTmpl.objects.get(id=main.tmplId)
        else:
            self.aCmdTemplates = NewTmpl.objects.all()
        logger.info('load %d cmd templates.' % len(self.aCmdTemplates))


class KtSender(threading.Thread):
    def __init__(self, builder):
        threading.Thread.__init__(self)
        self.builder = builder
        self.aDataFile = builder.aDataFile

        # self.aFiles = builder.aFiles
        # self.dFiles = builder.dFiles
        # self.kt = builder.makeKtClient('SENDER')
        self.orderQueue = builder.orderQueue
        # self.curPsid = self.conn.prepareSql(self.sqlPsid)

    def sendTmpl(self):
        for tpl in self.builder.aCmdTemplates:
            billId = tpl.bill_id
            regionCode = 1
            pass

    def run(self):
        aTmpl = self.builder.aCmdTemplates
        if len(self.aDataFile) == 0:
            logging.info('no data file, redo template.')
            for tpl in aTmpl:
                billArea = int(tpl.bill_id) // 10000
                regionCode = self.builder.dNumberArea[billArea]
                tableName = 'i_provision_%s' % regionCode
                Ps = ModelFac(tableName, PsBas)
                psId = PsIdSeq.objects.raw('select seq_ps_id.nextval as ps_id,seq_ps_donecode.nextval from dual')[0]
                logger.debug('ps_id: %d', psId.id)
                ps = Ps.create(psId, tpl)
                ps.save()
                self.orderQueue.put(ps, 1)
            return
        for fi in self.aDataFile:
            logging.info('process file %s', fi.dataFile)
            i = 0
            aFieldName = fi.aFields
            dPsData = fi.next()
            while (dPsData):
                if len(dPsData) < 1:
                    break
                i += 1
                if i > 199:
                    i = 0
                    time.sleep(3)
                    while self.orderQueue.qsize() > 1000:
                        logging.info('order queue size exceed 1000, sleep 10')
                        time.sleep(10)
                logging.debug(dPsData)
                billId = int(dPsData['bill_id'])
                billArea = billId // 10000
                regionCode = self.builder.dNumberArea[billArea]
                tableName = 'i_provision_%s' % regionCode
                Ps = ModelFac(tableName, PsBas)
                for tpl in self.builder.aCmdTemplates:
                    psId = PsIdSeq.objects.raw('select seq_ps_id.nextval as ps_id,seq_ps_donecode.nextval from dual')[0]
                    ps = Ps.create(psId, tpl)
                    for pa in dPsData:
                        if pa in ps.__dict__:
                            ps.pa = dPsData[pa]
                    ps.save()
                    ps.file = fi
                    self.orderQueue.put(ps, 1)
                dPsData = fi.next()
            logging.info('read %s complete, and delete.', fi)
            os.remove(fi)
        logger.info('sended all ps.')


class KtRecver(threading.Thread):
    def __init__(self, builder):
        threading.Thread.__init__(self)
        self.builder = builder
        # self.aFiles = builder.aFiles
        # self.dFiles = builder.dFiles
        # self.kt = builder.makeKtClient('RECVER')
        self.orderQueue = builder.orderQueue
        # self.dOrder = {}
        # self.doneOrders = {}
        # self.pattImsi = re.compile(r'IMSI1=(\d{15});')

    def run(self):
        time.sleep(10)
        emptyCounter = 0
        i = 0
        while 1:
            order = None
            if self.orderQueue.empty():
                emptyCounter += 1
                if emptyCounter > 20:
                    logging.info('exceed 20 times empty, exit.')
                    # for file in self.dFiles.keys():
                    #     aFileInfo = self.dFiles.pop(file)
                    #     self.dealFile(aFileInfo)
                    break
                time.sleep(30)
                continue
            i += 1
            if i > 199:
                i = 0
                time.sleep(3)
            ps = self.orderQueue.get(1)
            logging.debug('get ps %s %d from queue', ps.bill_id, ps.id)
            billArea = int(ps.bill_id) // 10000
            regionCode = self.builder.dNumberArea[billArea]
            createDate = ps.create_date
            tableMon = createDate.strftime('%Y%m')
            tableName = 'ps_provision_his_%s_%s' % (regionCode,tableMon)
            Ps = ModelFac(tableName, PsBas)
            psHis = Ps.objects.filter(id=ps.id)
            if len(psHis) == 0:
                self.orderQueue.put(ps, 1)
                continue
            strRsp = '%d %d %s %s' % (psHis.id, psHis.ps_status, psHis.bill_id, psHis.fail_reason)
            rspFile = None
            if 'file' in psHis.__dict__:
                rspFile = ps.file.rspFile
            else:
                rspFile = self.builder.cmdRsp
            rspFile.saveRsp(strRsp)
            if rspFile.rspTotal == rspFile.rspDone:
                logger.info('rspfile %s completed,', rspFile.rspFile)
                rspFile.back()

            time.sleep(1)
        logger.info('all completed.')

    def makeRsp(self, order):
        inFile = order.file
        fp = self.dFiles[inFile][4]
        sRsp = order.line
        for rsp in order.aResp:
            psId = rsp[0]
            psStatus = rsp[1]
            failReason = rsp[2]
            # sRsp = '%d %d %s %s' % (psId, psStatus, sRsp, failReason)
            sRsp = '%d %d %s %s' % (psId, psStatus, order.line, failReason)
            fp.write('%s%s' % (sRsp, os.linesep))
        # fp.write('%s%s' % (sRsp, os.linesep))
        logging.debug(sRsp)


class Director(object):
    def __init__(self):
        self.builder = None
        self.shutDown = None
        self.fRsp = None

    def makeBuilder(self):
        if main.facType == 't':
            self.builder = PsBuilder_TF()
        elif self.facType == 'f':
            self.builder = PsBuilder_FF()

    def start(self):
        self.makeBuilder()
        self.builder.loadCmd()
        if self.builder.inFile is not None:
            # print(self.factory.inFile)
            logger.info('find in files')
            self.builder.findFile()
        self.builder.loadNumberArea()
        self.builder.buildQueue()
        sender = self.builder.buildKtSender()
        recver = self.builder.buildKtRecver()

        logger.info('sender start.')
        sender.start()
        logger.info('recver start.')
        recver.start()

        sender.join()
        logger.info('sender complete.')
        recver.join()
        logger.info('recver complete.')


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        # self.conn = None
        # self.writeConn = None
        self.inFile = None
        self.cmdTpl = None
        self.tplFile = None
        self.fCmd = None
        self.netType = None
        self.netCode = None
        self.today = time.strftime("%Y%m%d", time.localtime())
        self.nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())

    def checkArgv(self):
        self.dirBin, self.appName = os.path.split(self.Name)
        self.appNameBody, self.appNameExt = os.path.splitext(self.appName)

        if self.argc < 2:
            self.usage()

        argvs = sys.argv[1:]
        self.facType = 't'
        self.cmdTpl = 'ps_model_summary'
        # self.tplFile = None
        self.fCmd = None
        self.tmplId = None
        self.inFile = None
        try:
            opts, arvs = getopt.getopt(argvs, "t:f:p:")
        except getopt.GetoptError as e:
            print('get opt error:%s. %s' % (argvs, e))
            self.usage()
        for opt, arg in opts:
            if opt == '-t':
                self.facType = 't'
                self.cmdTpl = arg
            elif opt == '-f':
                self.facType = 'f'
                self.cmdTpl = arg
            elif opt == '-p':
                self.tmplId = arg
        if len(arvs) > 0:
            self.inFile = arvs[0]

    def parseWorkEnv(self):
        self.dirApp = os.path.dirname(pathname)
        self.dirLog = os.path.join(self.dirApp, 'log')
        # self.dirCfg = os.path.join(self.dirApp, 'config')
        # self.dirCfg = self.dirBin
        self.dirBack = os.path.join(self.dirApp, 'back')
        self.dirIn = os.path.join(self.dirApp, 'input')
        self.dirLib = os.path.join(self.dirApp, 'lib')
        self.dirOut = os.path.join(self.dirApp, 'output')
        self.dirWork = os.path.join(self.dirApp, 'work')
        self.dirTpl = os.path.join(self.dirApp, 'template')

        logger.info('input dir: %s', self.dirIn)

        # cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logPre = '%s_%s' % (self.appNameBody, self.today)
        outName = '%s_%s' % (self.appNameBody, self.nowtime)
        # self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        # self.tplFile = os.path.join(self.dirTpl, self.cmdTpl)
        # self.logPre = os.path.join(self.dirLog, logPre)
        # self.outFile = os.path.join(self.dirOut, outName)
        if self.inFile:
            self.inFile = os.path.join(self.dirIn, self.inFile)
        if self.facType == 'f':
            self.cmdTpl = os.path.join(self.dirTpl, self.cmdTpl)

    def usage(self):
        print("Usage: %s [-t|f orderTmpl] [-p psid] [datafile]" % self.appName)
        print('option:')
        print('-t orderTmpl : 指定模板表orderTmpl，默认表是ps_model_summary')
        print('-f orderTmpl : 指定模板文件orderTmpl')
        print('-p psid :      取模板表中ps_id为psid的记录为模板，没有这个参数取整个表为模板')
        print('datafile :     数据文件，取里面的号码替换掉模板中的号码发开通')
        print("example:")
        print("\t%s pccnum" % (self.appName))
        print("\t%s -t ps_model_summary" % (self.appName))
        print("\t%s -t ps_model_summary -p 2451845353" % (self.appName))
        print("\t%s -t ps_model_summary -p 2451845353 pccnum" % (self.appName))
        print("\t%s -f kt_hlr" % (self.appName))
        print("\t%s -f kt_hlr pccnum" % (self.appName))
        exit(1)

    def openFile(self, fileName, mode):
        try:
            f = open(fileName, mode)
        except IOError as e:
            logger.fatal('open file %s error: %s', fileName, e)
            return None
        return f

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        logger.info('infile: %s' % self.inFile)
        director = Director()
        director.start()


def createInstance(module_name, class_name, *args, **kwargs):
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    class_meta = getattr(module_meta, class_name)
    obj = class_meta(*args, **kwargs)
    return obj


if __name__ == "__main__":
    # p = Press(title="hi", author="hi")
    # p.save()
    # PsHis1001901 = PsBas.setDb_table('ps_provision_his_100_201901')
    # p2453752950 = PsHis1001901.objects.get(id=2453752950)
    # print(p2453752950)
    main = Main()
    logger.info('%s starting...', os.path.basename(__file__))
    main.start()
    logger.info('%s complete.', os.path.basename(__file__))
