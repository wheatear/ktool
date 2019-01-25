# ktool/operation/sendkt.py
import os
import sys
import time
import datetime
import threading
import Queue
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

from operation.models import PsBas,PsTmpl,ModelFac,NumberArea
import ktool.settings

logger = logging.getLogger('django')
errlog = logging.getLogger('error')


class CmdFile(object):
    def __init__(self, file):
        self.cmdFile = file
        self.netType = None
        self.netCode = None
        self.aCmdTemplates = []
        self.dTmpl = {}

    def loadCmd(self):
        logger.info('loading cmd template %s', self.cmdFile)
        fCmd = main.openFile(self.cmdFile, 'r')
        tplFile = os.path.join(self.main.dirTpl, self.cmdTpl)
        fileName = os.path.basename(tplFile)
        tplName = os.path.splitext(fileName)[0]
        aCmdTmpl = []
        tmplCmd = {}
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
                if len(tmplCmd) > 0:
                    i += 1
                    tmplCmd['OLD_PS_ID'] = i
                    tmplCmd['PS_MODEL_NAME'] = tplFile
                    logger.info(tmplCmd)
                    aCmdTmpl.append(tmplCmd)
                    tmplCmd = None
                continue
            if line == 'KT_REQUEST':
                tmplCmd = {}
                continue
            aParam = line.split(' ', 1)
            if len(aParam) < 1:
                continue
            tmplCmd[aParam[0]] = aParam[1]
        self.aCmdTemplates = aCmdTmpl
        fCmd.close()
        # logger.info(self.aCmdTemplates)


class PsBuilder_FF(object):
    def __init__(self):
        self.inFile = main.inFile
        self.cmdTpl = main.cmdTpl

        self.aFiles = []
        self.dFiles = {}
        # self.orderDs = None
        self.aNetInfo = []
        self.dNetClient = {}
        # self.respName = '%s.rsp' % os.path.basename(self.main.outFile)
        # self.respFullName = os.path.join(self.main.dirOutput, self.respName)
        self.resp = None
        self.aCmdTemplates = []

    def findFile(self):
        logger.info('find data files ')
        if self.inFile:
            self.aFiles = glob.glob(self.inFile)
            if len(self.aFiles) == 0:
                logger.error('no find data file %s', self.inFile)
                # print('no find data file %s' % self.inFile)
                exit(-1)
            logger.info('find data files: %s', self.aFiles)
        else:
            logger.info('no data file')
        logger.info('find cmd file.')
        self.tmplFile = os.path.join(main.dirTpl, self.cmdTpl)
        logger.info('cmd file %s', self.tmplFile)

    def lineCount(self):
        if len(self.aFiles) == 0:
            fileBase = self.cmdTpl
            fileRsp = '%s.rsp' % self.cmdTpl
            fileWkRsp = os.path.join(main.dirWork, fileRsp)
            fWkRsp = main.openFile(fileWkRsp, 'w')
            fileOutRsp = os.path.join(main.dirOut, fileRsp)
            count = 1
            self.dFiles[fileBase] = [fileBase, count, self.aCmdTemplates, fileWkRsp, fWkRsp, fileOutRsp]
            logger.info('file: %s %d', fileBase, count)
            return self.dFiles
        for fi in self.aFiles:
            fileBase = os.path.basename(fi)
            nameBody,nameExt = os.path.splitext(fileBase)
            # cmdTpl = self.dFildCmdMap[nameExt]
            fileBack = os.path.join(main.dirBack, fileBase)
            shutil.copy(fi, fileBack)
            fileRsp = '%s.rsp' % fileBase
            fileWkRsp = os.path.join(main.dirWork, fileRsp)
            fWkRsp = self.main.openFile(fileWkRsp, 'w')
            fileOutRsp = os.path.join(main.dirOut, fileRsp)

            count = -1
            for count, line in enumerate(open(fi, 'rU')):
                pass
            # count += 1
            self.dFiles[fi] = [fileBase,count, self.aCmdTemplates, fileWkRsp, fWkRsp, fileOutRsp]
            logger.info('file: %s %d', fi, count)
            # self.dFileSize[fi] = count
        return self.dFiles

    def loadCmd(self):
        logger.info('loading cmd template %s', self.tmplFile)
        # tplFile = os.path.join(self.main.dirTpl, self.cmdTpl)
        # fileName = os.path.basename(tplFile)
        tplName = os.path.splitext(self.cmdTpl)[0]
        aCmdTmpl = []
        tmplCmd = {}
        i = 0
        fCmd = main.openFile(self.tmplFile,'r')
        for line in fCmd:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            if line == '$END$':
                if len(tmplCmd) > 0:
                    i += 1
                    tmplCmd['OLD_PS_ID'] = i
                    tmplCmd['PS_MODEL_NAME'] = self.tmplFile
                    logger.info(tmplCmd)
                    # tmpl = KtPsTmpl(tmplCmd)
                    aCmdTmpl.append(tmplCmd)
                    tmpl = None
                    tmplCmd = {}
                continue
            if line == 'KT_REQUEST':
                tmpl = None
                tmplCmd = {}
                continue
            aParam = line.split(' ', 1)
            if len(aParam) < 1:
                continue
            tmplCmd[aParam[0]] = aParam[1]
        fCmd.close()
        self.aCmdTemplates = aCmdTmpl
        # logger.info(self.aCmdTemplates)

    def makeKtClient(self, ktName):
        conn = self.makeConn(ktName)
        kt = KtClient(conn)
        kt.aCmdTemplates = self.aCmdTemplates
        return kt

    def buildQueue(self):
        self.orderQueue = Queue.Queue(1000)
        return self.orderQueue

    def buildKtSender(self):
        sender = KtSender(self)
        return sender

    def buildKtRecver(self):
        recver = KtRecver(self)
        return recver

    def openDs(self):
        if self.orderDs: return self.orderDs
        logger.info('open ds %s', self.orderDsName)
        self.orderDs = self.main.openFile(self.orderDsName, 'r')
        if self.orderDs is None:
            logger.fatal('Can not open orderDs file %s.', self.orderDsName)
            exit(2)
        return self.orderDs

    def closeDs(self):
        if self.orderDs:
            self.orderDs.close()

    def openRsp(self):
        if self.resp: return self.resp
        self.resp = self.main.openFile(self.respFullName, 'a')
        logger.info('open response file: %s', self.respName)
        if self.resp is None:
            logger.fatal('Can not open response file %s.', self.respName)
            exit(2)
        return self.resp

    def saveResp(self, order):
        for rsp in order.aResp:
            self.resp.write('%s %s%s' % (order.dParam['BILL_ID'], rsp, os.linesep))

    def makeOrderFildName(self):
        fildName = self.orderDs.readline()
        logger.info('field: %s', fildName)
        fildName = fildName.upper()
        self.aFildName = fildName.split()

    def makeOrder(self):
        orderClassName = '%sOrder' % self.netType
        logger.debug('load order %s.', orderClassName)
        for line in self.orderDs:
            line = line.strip()
            logger.debug(line)
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            aParams = line.split()

            order = createInstance(self.main.appNameBody, orderClassName)
            order.setParaName(self.aFildName)
            order.setPara(aParams)
            logger.debug('order param: %s', order.dParam)
            # netCode = self.aNetInfo[0]['NetCode']
            order.net = self.dNetClient[self.netCode]
            return order
        return None


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
        self.aFiles = builder.aFiles
        self.dFiles = builder.dFiles
        self.kt = builder.makeKtClient('SENDER')
        self.orderQueue = builder.orderQueue
        # self.curPsid = self.conn.prepareSql(self.sqlPsid)

    def makeOrderFildName(self, fIn):
        fildName = fIn.readline()
        logging.info('field: %s', fildName)
        fildName = fildName.upper()
        aFildName = fildName.split()
        return aFildName

    def sendTmpl(self):
        for tpl in self.builder.aCmdTemplates:
            billId = tpl.bill_id
            regionCode = 1
            pass

    def run(self):
        if len(self.aFiles) == 0:
            logging.info('no data file, redo template.')
            line = ''
            fi = self.builder.main.cmdTpl
            order = KtOrder(line, fi)
            self.kt.sendOrder(order)
            self.orderQueue.put(order, 1)
            return

        for fi in self.aFiles:
            # fileBody, fileExt = os.path.splitext(fi)
            # aCmdTpl = self.builder.dFildCmdMap[fileExt]
            logging.info('process file %s', fi)
            aTpl = self.dFiles[fi][2]
            self.kt.aCmdTemplates = self.builder.aCmdTemplates

            i = 0
            fp = self.main.openFile(fi, 'r')
            aFieldName = self.makeOrderFildName(fp)
            for line in fp:
                line = line.strip()
                if len(line) < 1:
                    continue
                i += 1
                if i > 199:
                    i = 0
                    time.sleep(3)
                    while self.orderQueue.qsize() > 1000:
                        logging.info('order queue size exceed 1000, sleep 10')
                        time.sleep(10)
                logging.debug(line)
                aPara = line.split()
                # billId = aPara[2]
                # subBillId = aPara[1]
                # aFieldName = ['BILL_ID', 'SUB_BILL_ID']
                # aKtPara = [billId, subBillId]
                order = KtOrder(line, fi)
                order.setParaName(aFieldName)
                order.setPara(aPara)
                self.kt.sendOrder(order)
                self.orderQueue.put(order, 1)
            fp.close()
            logging.info('read %s complete, and delete.', fi)
            os.remove(fi)


class KtRecver(threading.Thread):
    def __init__(self, builder):
        threading.Thread.__init__(self)
        self.builder = builder
        self.main = builder.main
        self.aFiles = builder.aFiles
        self.dFiles = builder.dFiles
        self.kt = builder.makeKtClient('RECVER')
        self.orderQueue = builder.orderQueue
        self.dOrder = {}
        self.doneOrders = {}
        # self.pattImsi = re.compile(r'IMSI1=(\d{15});')

    def run(self):
        if len(self.dFiles) == 0:
            logging.info('no file')
            return
        for file in self.dFiles:
            self.doneOrders[file] = 0
        time.sleep(10)
        emptyCounter = 0
        i = 0
        while 1:
            order = None
            if len(self.dFiles) == 0:
                logging.info('all files completed.')
                break
            if self.orderQueue.empty():
                emptyCounter += 1
                if emptyCounter > 20:
                    logging.info('exceed 20 times empty, exit.')
                    for file in self.dFiles.keys():
                        aFileInfo = self.dFiles.pop(file)
                        self.dealFile(aFileInfo)
                    break
                time.sleep(30)
                continue
            i += 1
            if i > 199:
                i = 0
                time.sleep(3)
            order = self.orderQueue.get(1)
            logging.debug('get order %s %d from queue', order.aReq[0]['BILL_ID'], order.aReq[0]['PS_ID'])
            self.kt.recvOrder(order)
            if len(order.aWaitPs) > 0:
                self.orderQueue.put(order, 1)
                continue
            self.makeRsp(order)
            inFile = order.file
            self.doneOrders[inFile] += 1
            logging.debug('done: %d; all: %d', self.doneOrders[inFile], self.dFiles[inFile][1])
            if self.doneOrders[inFile] == self.dFiles[inFile][1]:
                logging.info('file %s completed after process %d lines.', inFile, self.doneOrders[inFile])
                aFileInfo = self.dFiles.pop(inFile)
                self.dealFile(aFileInfo)
            time.sleep(1)

    def checkSub(self, orderRsp, inFile):
        rsp = orderRsp[3]
        m = None
        m = self.pattImsi.search(rsp)
        fp = None
        if m:
            logging.debug(m.groups())
            fp = self.dFiles[inFile][6]
        else:
            logging.debug('no find %s', orderRsp[0])
            fp = self.dFiles[inFile][7]
        return fp

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

    def dealFile(self, aFileInfo):
        # self.dFiles[fi] = [fileBase,count, cmdTpl, fileWkRsp, fWkRsp, fileOutRsp]
        fWkRsp = aFileInfo[4]
        fWkRsp.close()
        fileWkRsp = aFileInfo[3]
        fileOutRsp = aFileInfo[5]
        fileBkRsp = os.path.join(self.main.dirBack, os.path.basename(fileWkRsp))
        shutil.copy(fileWkRsp, fileBkRsp)
        os.rename(fileWkRsp, fileOutRsp)
        logging.info('%s complete', aFileInfo[0])


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
        if self.builder.inFile is not None:
            # print(self.factory.inFile)
            logger.info('find in files')
            self.builder.findFile()
            self.builder.lineCount()
        self.builder.loadCmd()
        # self.factory.buildKtClient()
        queue = self.builder.buildQueue()
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
            print 'get opt error:%s. %s' % (argvs, e)
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
        if self.dirBin=='' or self.dirBin=='.':
            self.dirBin = '.'
            self.dirApp = '..'
        else:
            dirApp, dirBinName = os.path.split(self.dirBin)
            if dirApp=='':
                self.dirApp = '.'
            else:
                self.dirApp = dirApp
        self.dirLog = os.path.join(self.dirApp, 'log')
        # self.dirCfg = os.path.join(self.dirApp, 'config')
        # self.dirCfg = self.dirBin
        self.dirBack = os.path.join(self.dirApp, 'back')
        self.dirIn = os.path.join(self.dirApp, 'input')
        self.dirLib = os.path.join(self.dirApp, 'lib')
        self.dirOut = os.path.join(self.dirApp, 'output')
        self.dirWork = os.path.join(self.dirApp, 'work')
        self.dirTpl = os.path.join(self.dirApp, 'template')

        # cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logPre = '%s_%s' % (self.appNameBody, self.today)
        outName = '%s_%s' % (self.appNameBody, self.nowtime)
        # tplName = '*.tpl'
        # self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        # self.tplFile = os.path.join(self.dirTpl, self.cmdTpl)
        # self.logPre = os.path.join(self.dirLog, logPre)
        # self.outFile = os.path.join(self.dirOut, outName)
        if self.inFile:
            self.inFile = os.path.join(self.dirIn, self.inFile)

    def usage(self):
        print "Usage: %s [-t|f orderTmpl] [-p psid] [datafile]" % self.appName
        print('option:')
        print(u'-t orderTmpl : 指定模板表orderTmpl，默认表是ps_model_summary'.encode('gbk'))
        print(u'-f orderTmpl : 指定模板文件orderTmpl'.encode('gbk'))
        print(u'-p psid :      取模板表中ps_id为psid的记录为模板，没有这个参数取整个表为模板'.encode('gbk'))
        print(u'datafile :     数据文件，取里面的号码替换掉模板中的号码发开通'.encode('gbk'))
        print "example:"
        print "\t%s pccnum" % (self.appName)
        print "\t%s -t ps_model_summary" % (self.appName)
        print "\t%s -t ps_model_summary -p 2451845353" % (self.appName)
        print "\t%s -t ps_model_summary -p 2451845353 pccnum" % (self.appName)
        print "\t%s -f kt_hlr" % (self.appName)
        print "\t%s -f kt_hlr pccnum" % (self.appName)
        exit(1)

    def openFile(self, fileName, mode):
        try:
            f = open(fileName, mode)
        except IOError as e:
            logger.fatal('open file %s error: %s', fileName, e)
            return None
        return f

    # def connectServer(self):
    #     if self.conn is not None: return self.conn
    #     self.conn = DbConn('main', self.cfg.dbinfo)
    #     self.conn.connectServer()
    #     return self.conn

    # def getConn(self, connId):
    #     conn = DbConn(connId, self.cfg.dbinfo)
    #     return conn

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
    logger.info('%s starting...' % main.appName)
    main.start()
    logger.info('%s complete.', main.appName)
