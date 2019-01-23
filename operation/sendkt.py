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

pathname = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, pathname)
sys.path.insert(0, os.path.abspath(os.path.join(pathname, '..')))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ktool.settings")

import django
django.setup()

from operation.models import PsBas,PsTmpl
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


class KtPsFFac(object):
    def __init__(self, inFile, cmdTpl):
        # self.main = main
        # self.netType = main.netType
        # self.netCode = main.netCode
        self.inFile = inFile
        self.cmdTpl = cmdTpl

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
        logger.info('find files ')
        if self.inFile:
            self.aFiles = glob.glob(self.inFile)
            if len(self.aFiles) == 0:
                logger.error('no find data file %s', self.inFile)
                print('no find data file %s' % self.inFile)
                exit(-1)
            logger.info('find files: %s', self.aFiles)
        else:
            logger.info('no data file')
        return self.aFiles

    def lineCount(self):
        if len(self.aFiles) == 0:
            fileBase = self.cmdTpl
            fileRsp = '%s.rsp' % self.cmdTpl
            fileWkRsp = os.path.join(self.main.dirWork, fileRsp)
            fWkRsp = self.main.openFile(fileWkRsp, 'w')
            fileOutRsp = os.path.join(self.main.dirOut, fileRsp)
            count = 1
            self.dFiles[fileBase] = [fileBase, count, self.aCmdTemplates, fileWkRsp, fWkRsp, fileOutRsp]
            logger.info('file: %s %d', fileBase, count)
            return self.dFiles
        for fi in self.aFiles:
            fileBase = os.path.basename(fi)
            nameBody,nameExt = os.path.splitext(fileBase)
            # cmdTpl = self.dFildCmdMap[nameExt]
            fileBack = os.path.join(self.main.dirBack, fileBase)
            shutil.copy(fi, fileBack)
            fileRsp = '%s.rsp' % fileBase
            fileWkRsp = os.path.join(self.main.dirWork, fileRsp)
            fWkRsp = self.main.openFile(fileWkRsp, 'w')
            fileOutRsp = os.path.join(self.main.dirOut, fileRsp)

            count = -1
            for count, line in enumerate(open(fi, 'rU')):
                pass
            # count += 1
            self.dFiles[fi] = [fileBase,count, self.aCmdTemplates, fileWkRsp, fWkRsp, fileOutRsp]
            logger.info('file: %s %d', fi, count)
            # self.dFileSize[fi] = count
        return self.dFiles

    def loadCmd(self):
        logger.info('loading cmd template %s', self.cmdTpl)
        tplFile = os.path.join(self.main.dirTpl, self.cmdTpl)
        fileName = os.path.basename(tplFile)
        tplName = os.path.splitext(fileName)[0]
        aCmdTmpl = []
        tmplCmd = {}
        i = 0
        fCmd = self.main.fCmd
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
                    tmplCmd['PS_MODEL_NAME'] = tplFile
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
        self.aCmdTemplates = aCmdTmpl
        # logger.info(self.aCmdTemplates)

    def makeConn(self, connId):
        conn = DbConn(connId, self.main.cfg.dbinfo)
        return conn

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


class TableFac(KtPsFFac):
    dSql = {}
    dSql['LOADTMPL'] = 'select ps_id,region_code,bill_id,sub_bill_id,ps_service_type,action_id,ps_param from %s order by create_date,ps_id'
    dSql['LOADTMPLBYPS'] = 'select ps_id,region_code,bill_id,sub_bill_id,ps_service_type,action_id,ps_param from %s where ps_id=:PS_ID'
    dCur = {}
    def __init__(self, main):
        super(self.__class__, self).__init__(main)
        # self.respName = '%s_rsp' % os.path.basename(self.main.outFile)
        # self.respFullName = self.respName
        self.conn = main.conn
        self.cmdTab = self.main.cmdTpl

    def getCurbyName(self, curName):
        if curName in self.dCur: return self.dCur[curName]
        if curName not in self.dSql:
            return None
        sql = self.dSql[curName] % self.cmdTab
        cur = self.conn.prepareSql(sql)
        self.dCur[curName] = cur
        return cur

    def loadCmd(self):
        tmpl = None
        tmplMsg = ''
        # sql = 'select ps_id,region_code,bill_id,sub_bill_id,ps_service_type,action_id,ps_param from %s where status=1 order by sort' % self.cmdTab
        logger.info('load cmd template.')
        para = None
        if self.main.tmplId:
            cur = self.getCurbyName('LOADTMPLBYPS')
            para = {'PS_ID': self.main.tmplId}
        else:
            cur = self.getCurbyName('LOADTMPL')
        self.conn.executeCur(cur, para)
        rows = self.conn.fetchall(cur)
        for line in rows:
            cmd = {}
            for i,field in enumerate(cur.description):
                cmd[field[0]] = line[i]
            cmd['OLD_PS_ID'] = cmd['PS_ID']
            cmd['PS_MODEL_NAME'] = self.cmdTab
            self.aCmdTemplates.append(cmd)
            # logger.info(line)
            logger.info('cmd template: %s', cmd)
        cur.close()
        logger.info('load %d cmd templates.' % len(self.aCmdTemplates))


class Director(object):
    def __init__(self, fac):
        self.factory = fac
        self.shutDown = None
        self.fRsp = None

    def start(self):
        if self.factory.inFile is not None:
            # print(self.factory.inFile)
            logger.info('find in files')
            self.factory.findFile()
            self.factory.lineCount()
        self.factory.loadCmd()
        # self.factory.buildKtClient()
        queue = self.factory.buildQueue()
        sender = self.factory.buildKtSender()
        recver = self.factory.buildKtRecver()

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
        self.tplFile = os.path.join(self.dirTpl, self.cmdTpl)
        # self.logPre = os.path.join(self.dirLog, logPre)
        # self.outFile = os.path.join(self.dirOut, outName)
        if self.inFile:
            self.inFile = os.path.join(self.dirIn, self.inFile)
        if self.cmdTpl:
            self.cmdTpl = os.path.join(self.dirTpl, self.cmdTpl)

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

    def makeFactory(self):
        if self.facType == 't':
            return self.makeTableFactory()
        elif self.facType == 'f':
            return self.makeFileFactory()

    def makeTableFactory(self):
        self.netType = 'KtPs'
        self.netCode = 'kt4'
        logger.info('net type: %s  net code: %s', self.netType, self.netCode)
        fac = TableFac(self)
        return fac

    def makeFileFactory(self):
        if not self.fCmd:
            self.fCmd = self.openFile(self.tplFile, 'r')
            logger.info('cmd template file: %s', self.tplFile)
            if not self.fCmd:
                logger.fatal('can not open command file %s. exit.', self.tplFile)
                exit(2)

        for line in self.fCmd:
            if line[:8] == '#NETTYPE':
                aType = line.split()
                self.netType = aType[2]
            if line[:8] == '#NETCODE':
                aCode = line.split()
                self.netCode = aCode[2]
            if self.netType and self.netCode:
                logger.info('net type: %s  net code: %s', self.netType, self.netCode)
                break
        logger.info('net type: %s  net code: %s', self.netType, self.netCode)
        if self.netType is None:
            logger.fatal('no find net type,exit.')
            exit(3)
        # facName = '%sFac' % self.netType
        # fac_meta = getattr(self.appNameBody, facName)
        # fac = fac_meta(self)
        facName = '%sFFac' % self.netType
        fac = createInstance(self.appNameBody, facName, self)
        # fac = FileFac(self)
        return fac

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        logger.info('infile: %s' % self.inFile)

        factory = self.makeFactory()
        # print('respfile: %s' % factory.respFullName)
        # builder = Builder(self)
        director = Director(factory)
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
