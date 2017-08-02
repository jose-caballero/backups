"""
# update_from_agis4.py ... Update ATLAS 'Sites' information from AGIS json.
#
#
#
# Copyright European Organization for Nuclear Research (CERN)
#
# This software is distributed under the terms of the
# GNU General Public Licence version 3 (GPL Version 3), copied verbatim in the
# file COPYING.
#
# In applying this licence, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as an Intergovernmental Organization or
# submit itself to any jurisdiction.
#
#
# Authors:
# - Valentina Mancinelli, <valentina.mancinelli@cern.ch>, 2015-2016
# - Jaroslava Schovancova, <jaroslava.schovancova@cern.ch>, 2016-2017
# - Jose Caballero, <jcaballero@bnl.gov>, 2017
#
#
"""

### setup Django properly in 1.11LTS. HC-648
import django
django.setup()

import collections
import commands
import datetime
import getopt
import os
import shlex
import json as simplejson
import subprocess
import sys
import traceback
import urllib2
from collections import defaultdict
from pprint import pprint
### Disable PYTHONPATH prints. HC-514
# print os.environ['PYTHONPATH'] ###FIXME
from hc.atlas.models import Backend, Cloud, Site, SiteOption, Template, TemplateSite
from hc.core.utils.generic.class_func import custom_import
from hc.core.utils.hc import cernmail
from hc.core.utils.hc.hc_logging import logger
# from django.db import transaction

from lib import filemover # it is in /data/hc/apps/atlas/python/lib/filemover.py


### production run: simulation=False
simulation = False #FIXME
### test run: simulation=True
# simulation = True #FIXME

### configure logger
LOG_DIR="/data/hc/apps/atlas/logs"
LOGGER_NAME="update_from_agis4"
# try:
#     LOGGER_NAME=os.path.splitext(os.path.basename(__file__))[0]
# except:
#     LOGGER_NAME=os.path.basename(__file__)
LOGGER_LEVEL='debug'
LOGGER_FILE=os.path.join(LOG_DIR, LOGGER_NAME)+'.log'
_logger = logger(LOGGER_NAME, LOGGER_LEVEL, LOGGER_FILE)

_logger.debug('Configured logger of %s into %s' %(LOGGER_NAME, LOGGER_FILE))


def mail_failure():
    """send an email containing the traceback of the error"""
    to = 'hammercloud-alarms-atlas@cern.ch,hammercloud-notifications-atlas@cern.ch'
    lines = ['The AGIS update script failed with the following exception:','']
    import traceback
    try:
        lines.append(traceback.format_exc())

        body = '\n'.join(lines)
        subject = '[HC-UPDATE-FROM-AGIS] AGIS update script failed at ' + str(datetime.datetime.now())
        cernmail.send(to, subject, body)
    except:
        _logger.error('error sending notification mail')


class UpdateFromAgis:
    """Script to update HC database from AGIS"""

    def __init__(self):
        """
        UpdateFromAgis

        For each PanDA resource we need:
            PanDA resource name
                + PanDA site
                + ATLAS site
                capability
                type
                hc_param
                is_default

        For each ATLAS site we need:
            ATLAS site name
                + cloud
                + ddmendpoints.keys() ... RSEs

        Output data structures:
            ### active PanDA resources, that have hc_param != False
            self.resources = resources
            ### all PanDA resources
            self.pandaresources = pandaresources
            self.pandaqueues = pandaqueues

        We calculate following boolean flags for each PanDA resource:
            is_mcore
            is_himem
            is_standalone
            is_slave
        However, it would be better to use the AGIS fields directly:
            * capability: SCORE, MCORE, HIMEM, etc.
            * slave vs. master: is_default and PanDA site

        """
        self._logger=_logger
        self.ddm_changes = []
        self.autoexclusion_changes = []
        self.template_changes = []
        self.ddm_allowed_tokens = ['DATADISK', 'LOCALGROUPDISK', 'PRODDISK']
        self.ALLOWED_ATLASSITES = ['CERN-PROD'] #### for debug
        self.ALLOWED_PANDARESOURCESS = ['ANALY_CERN_SHORT'] #### for debug
        self.ALLOWED_HC_SUITES = ['PFT', 'PFT_MCORE', 'AFT']

        self.agis_sites_json_url = "http://atlas-agis-api.cern.ch/request/site/query/list/?json&state=ACTIVE&rc_site_state=ACTIVE"
        self.agis_schedconf_json_url = "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all"
        self.agis_endpoint = 'atlas-agis-api.cern.ch'
#         self.agis_sites_json_url = "http://atlas-agis-api.cern.ch/request/site/query/list/?json&state=ACTIVE&rc_site_state=ACTIVE&site=CERN-PROD"  #### for debug
#         self.agis_schedconf_json_url = "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&panda_resource=ANALY_CERN_SHORT"  #### for debug
        self._logger.debug('Will download json %s' % (self.agis_schedconf_json_url))
        pandaqueues = simplejson.load(urllib2.urlopen(self.agis_schedconf_json_url))
        self._logger.debug('Finished download json %s' % (self.agis_schedconf_json_url))
        self._logger.debug('Will download json %s' % (self.agis_sites_json_url))
        json = simplejson.load(urllib2.urlopen(self.agis_sites_json_url))
        self._logger.debug('Finished download json %s' % (self.agis_sites_json_url))



        if os.getuid() == 0: # running as root
            vardir = '/var/run/hc/'
        else:
            import pwd
            vardir = '%s/var/run/hc/' %pwd.getpwuid(os.getuid()).pw_dir
        if not os.path.isdir(vardir):
            os.makedirs(vardir) 
            
        self.file_pr='/tmp/agis_pandaresources.json'
        self.filetxt_mcore = os.path.join(vardir, 'agis_mcore_sites.txt')
        self.filetxt_slave = os.path.join(vardir, 'agis_slave_sites.txt')
        self.file_mcore = os.path.join(vardir, 'agis_mcore_sites.json')
        self.file_slave = os.path.join(vardir, 'agis_slave_sites.json')
        self.file_nomaster = os.path.join(vardir, 'agis_no-master_sites.json')

        self.too_short_rse_list = 25

        self.pandaresources = []

#         for atlassite in [x for x in json if x['name'] in self.ALLOWED_ATLASSITES]:
        for atlassite in json:
            ### Get ATLAS site name
            atlassitename = atlassite['name']
            ### GET cloud for ATLAS site
            atlassitecloud = atlassite['cloud']
            ### Get ATLAS site RSEs
            atlassiteddm_list = self.get_list_RSE(atlassite)
            atlassiteddm=','.join(self.get_list_RSE(atlassite))
            ### Get PanDA site/resource
            atlassitepanda = atlassite['presources']
            ### Get ATLAS site datapolicies, for nucleus HC-435
            atlassitedatapolicies = atlassite['datapolicies']
            is_nucleus = 'Nucleus' in atlassitedatapolicies
            ### Get PanDA sites
            pandasites=atlassitepanda.keys()
            ### Get PanDA resources
            for pandasite in pandasites:
                pandaresources=atlassitepanda[pandasite].keys()
                panda_site_full=atlassitepanda[pandasite]
#                 pr_default=self.get_default_resource(panda_site_full)
#                 for pandaresource in [x for x in pandaresources \
#                                       if x in self.ALLOWED_PANDARESOURCESS]:
                for pandaresource in pandaresources:
                    pr_full=atlassitepanda[pandasite][pandaresource]
                    ### PanDA resource properties
                    pr_name=pandaresource
                    pr_capability=pr_full['capability']
                    pr_hc_param=pr_full['hc_param']
                    pr_is_default=pr_full['is_default']
                    pr_type=pr_full['type']
                    ### collect nucleus info HC-435
                    pr_is_nucleus = is_nucleus
                    if pr_is_nucleus:
                        pr_nucleus_name = atlassitename
                    else:
                        pr_nucleus_name = ''
                    ### collect PanDA queue name HC-360
#                     pr_queue=pr_full['name']
                    try:
                        pq = [pandaqueues[x] for x in pandaqueues if pandaqueues[x]['panda_resource'] == pr_name]
                        if len(pq):
                            pq=pq[0]
                    except:
                        pq = None
                        traceback.print_exc()
                        self._logger.error('site=%s traceback: %s' %(pr_name, repr(sys.exc_info())))
                    if pq is not None:
                        pr_queue = pq['nickname']
                    else:
                        pr_queue = pr_name
                    ### ddm field in AGIS is now the only source of information, do not hardcode anything. HC-359
# #                     pr_ddm = ','.join([pandaqueues[pr_queue]['ddm'], atlassiteddm])
#                     pq_ddm_list = pandaqueues[pr_queue]['ddm'].split(',')
#                     pr_ddm_tmp=[]
#                     pr_ddm = atlassiteddm
#                     if pq_ddm_list is not None:
#                         pr_ddm_tmp = pq_ddm_list
#                         if atlassiteddm_list is not None:
#                             pr_ddm_tmp += atlassiteddm_list
#                     pr_ddm = ','.join(pr_ddm_tmp)
                    pr_ddm = pandaqueues[pr_queue]['ddm']
                    ### collect hc_suite HC-410
                    pr_hc_suite = ''
                    try:
                        pr_hc_suite = ','.join(pandaqueues[pr_queue]['hc_suite'])
                    except:
                        self._logger.error('Failed to get hc_suite for PanDA resource "%s" (keys: %s). %s'%(pr_name, pandaqueues[pr_queue].keys(),repr(sys.exc_info())))
                        traceback.print_exc()
                    ### collect core_count HC-594
                    pr_corecount = pandaqueues[pr_queue]['corecount']
                    try:
                        pr_corecount = int(pr_corecount)
                    except:
                        pr_corecount = 1
                        self._logger.error('exception, setting pr_corecount to 1: %s'% (repr(sys.exc_info())))
                        traceback.print_exc()
                    ### tune cloud
                    cloud_code=self.get_cloud_code(atlassitecloud, pr_type)
                    site_cloud = Cloud.objects.get(code=cloud_code)
                    ### tune backend
                    site_backend = Backend.objects.get(name="PanDA")
                    ### tune monitoring link
                    site_monitoring_link = self.get_monitoring_link(pr_name, pr_type, pr_capability)
                    ### dict for Site object
                    pr_data_static = {'name': pr_name, \
                                    'cloud': site_cloud, \
                                    'backend': site_backend, \
                                    'ddm': pr_ddm, \
                                    'queue': pr_queue, \
                                    'enabled': True, \
                                    'monitoring_link': site_monitoring_link, \
                                    'real_name': atlassitename, \
                                    'description': "", \
                                    'alternate_name': "", \
                                    'capability': pr_capability, \
                                    'is_default': pr_is_default, \
                                    'hc_param': pr_hc_param, \
                                    'category': pr_type, \
                                    'is_nucleus': pr_is_nucleus, \
                                    'nucleus': pr_nucleus_name, \
                                    'hc_suite': pr_hc_suite
                                    }
                    ### additional data for PanDA resource which is not in Site table
                    pr_data_dynamic={}
                    pr_data_dynamic.update(pr_data_static)
                    pr_data_dynamic['queue_status'] = pandaqueues[pr_queue]['status']
                    pr_data_dynamic['panda_comment'] = pandaqueues[pr_queue]['comment']
                    pr_data_dynamic['is_standalone'] = pr_is_default
                    pr_data_dynamic['is_slave'] = not pr_is_default
                    pr_data_dynamic['is_mcore'] = self.is_multicore(pr_capability)
                    pr_data_dynamic['is_himem'] = self.is_highmem(pr_capability)
                    pr_data_dynamic['default_resource'] = self.get_default_resource_per_type(panda_site_full, pr_type)
                    pr_data_dynamic['robot_panda_queue'] = pandaqueues[pr_queue]['queue']
                    pr_data_dynamic['corecount'] = pr_corecount

                    ### save PanDA resource properties
                    self.pandaresources.append(pr_data_dynamic)
                    self._logger.debug('Add: ATLAS site %s PanDA site %s PanDA resource %s : %s' % (atlassitename, pandasite, pr_name, self.pandaresources[-1]))


    def get_default_resource(self, panda_site_full):
        """
        get_default_resource ... get a master PanDA resource of all PanDA resources of a PanDA site
        """
        res=None
        try:
            res=[x for x in panda_site_full \
                 if panda_site_full[x]['is_default']==True]
            return sorted(res)[0]
        except:
            pass
        return res


    def get_default_resource_per_type(self, panda_site_full, pr_type):
        """
        get_default_resource_per_type ... get a master PanDA resource of all PanDA resources of a PanDA site of a given resource type
        """
        res=None
        try:
            res=[x for x in panda_site_full \
                 if panda_site_full[x]['is_default']==True and panda_site_full[x]['type']==pr_type]
            return sorted(res)[0]
        except:
            pass
        return res


    def get_list_RSE(self, atlas_site):
        """
        get_list_RSE ... get list of RSEs at an ATLAS site
        """
        rses=[]
        try:
            rses = [x for x in atlas_site['ddmendpoints'] \
                    if atlas_site['ddmendpoints'][x]['state']=='ACTIVE' \
                    and any(token in x for token in self.ddm_allowed_tokens)]
        except:
            name=None
            try:
                name=atlas_site['name']
            except:
                pass
            self._logger.error("Cannot get list of RSEs for site "+name)
        return rses


    def get_cloud_code(self, cloud, pr_type):
        """
        get_cloud_code ... get HC code for the cloud
        """
        cloud_code=None
        tmp_cloud=cloud

        ### exception for CERN cloud
        if tmp_cloud == "CERN":
            tmp_cloud = 'T0'
        ### exception for ND cloud
        elif tmp_cloud == "ND":
            tmp_cloud = 'NG'

        if pr_type == 'analysis':
            cloud_code = tmp_cloud + "_PANDA"
        else:
            cloud_code = tmp_cloud + "_PROD"

        ### exception for US cloud, analy sites
        if cloud_code == "US_PANDA":
            cloud_code = "US"

        return cloud_code



    def add_site(self, pr_full):
        """add a site to the database"""
        site_name=pr_full['name']
        self._logger.info('Adding %s to HC DB' % (site_name))
        self._logger.info('Cloud %s' % (pr_full['cloud']))
        self._logger.info('RSEs %s' % (pr_full['ddm']))
        app = 'atlas'

        try:
            s = Site.objects.get(name=site_name)
        except Site.DoesNotExist:
            self._logger.info('Site not yet in DB, will add: %s' % (site_name))

        else:
            self._logger.error('[%s][add_site] site %s already in DB.' % (app, site_name))
            return

        self._logger.info('Site %s , Cloud %s , %s' % (site_name, pr_full['cloud'], pr_full))
        newsite = Site(name=pr_full['name'], \
                        cloud=pr_full['cloud'], \
                        backend=pr_full['backend'], \
                          ddm=pr_full['ddm'], \
                          queue=pr_full['queue'], \
                          enabled=pr_full['enabled'], \
                          monitoring_link=pr_full['monitoring_link'], \
                          real_name=pr_full['real_name'], \
                          description=pr_full['description'], \
                          alternate_name=pr_full['alternate_name'], \
                          capability=pr_full['capability'], \
                          is_default=pr_full['is_default'], \
                          hc_param=pr_full['hc_param'], \
                          category=pr_full['category'] \
                          )

        try:
            if not simulation:
                newsite.save()
            self._logger.info('Site %s added.' % (site_name))
        except:
            self._logger.error('[%s][add_site] Error adding site %s in DB.' % (app, site_name))
            return 1

        self.panda_setup(site_name)
        return 1


    def get_monitoring_link(self, pr_name, pr_type, pr_capability):
        """return a link to BigPanDAmon showing recent jobs at this queue"""
        isanaly = pr_type == 'analysis'
        ismcore = self.is_multicore(pr_capability)

        if isanaly:
            processingType = 'gangarobot-mcore' if ismcore else 'gangarobot'
            hours = 12
            monitoring_link = 'http://bigpanda.cern.ch/jobs/?computingsite=%(queuename)s&type=analysis&hours=%(hours)s&processingType=%(processingType)s'  \
                % {'queuename': pr_name, 'hours': hours, 'processingType': processingType}
        else:
            processingType = 'gangarobot-mcore' if ismcore else 'gangarobot-pft'
            hours = 24 if ismcore else 4
            monitoring_link = 'http://bigpanda.cern.ch/jobs/?computingsite=%(queuename)s&processingtype=%(processingType)s&hours=%(hours)s'  \
                % {'queuename': pr_name, 'hours': hours, 'processingType': processingType}

        return monitoring_link


    def set_autoexclusion(self, queuename, enable = True, pr_type="special"):
        """set the autoexclusion policy for this queue"""
        option_value =  'enable' if enable else 'disable'
        changed = False
        if not simulation:
            try:
                site = Site.objects.get(name = queuename)
                changed = SiteOption.set_option(site, 'autoexclusion', option_value)
                if changed:
                    self._logger.info('Changing autoexclusion for site %s to %s' % (queuename, option_value))
            except Site.DoesNotExist:
                self._logger.error('Site does not exist in db: %s (type=%s)' % (queuename, pr_type))
        return changed


    def panda_setup(self, queuename):
        """setup flags in panda for a new queue"""
        new_status='test'
        X509_CERT_DIR = os.environ['X509_CERT_DIR']
        X509_USER_PROXY = os.environ['X509_USER_PROXY']

        ### prepare for switch-off of PanDA controller. HC-628 HC-637 HC-640

#         self._logger.info('Setting up PanDA')
#         ### PanDA controller
#         try:
#             comm = "curl --capath %s --cacert %s --cert %s 'https://panda.cern.ch:25943/server/controller/query?moduser=HammerCloud&tpmes=setmanual&queue=%s&comment=HC.Test.Me' " %  (X509_CERT_DIR, X509_USER_PROXY, X509_USER_PROXY, queuename) #FIXME
#             self._logger.info('Executing: %s' % (comm))
#             if not simulation:
#     #             subprocess.check_call(shlex.split(comm))
#                 err, out = commands.getstatusoutput(comm)
#                 if err != 0:
#                     self._logger.error('Failed to run command %s: %s' % (comm, out))
#                 else:
#                     self._logger.debug('Output of command "%s" : %s' % (comm, out))
#             comm = "curl --capath %s --cacert %s --cert %s 'https://panda.cern.ch:25943/server/controller/query?moduser=HammerCloud&tpmes=settest&queue=%s&comment=HC.Test.Me' " % (X509_CERT_DIR, X509_USER_PROXY, X509_USER_PROXY, queuename)
#             self._logger.info('Executing: %s' % (comm))
#             if not simulation:
#     #             subprocess.check_call(shlex.split(comm))
#                 err, out = commands.getstatusoutput(comm)
#                 if err != 0:
#                     self._logger.error('Failed to run command %s: %s' % (comm, out))
#                 else:
#                     self._logger.debug('Output of command "%s" : %s' % (comm, out))
#         except:
#             self._logger.critical('PanDA controller call failed for some reason with site=%s new_status=%s: %s' % (queuename, new_status, repr(sys.exc_info())))

        ### AGIS controller.
        self._logger.info('Setting up PanDA via AGIS controller')
        if not simulation:
    #         expiration = urllib2.quote((datetime.datetime.utcnow() + datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M'), '')
            ### set proper reason! HC-639
#             reason = urllib2.quote('HammerCloud PFT test suite', '')
            reason = urllib2.quote('HC.Blacklist.set.%s' % (new_status), '')
            ### Remove expiration from AGIS controller queries. HC-378
            ### change curl options, so that it works: remove -k option. HC713
#             comm = "curl -s -k --cert $X509_USER_PROXY 'https://%s/request/pandaqueuestatus/update/set_probestatus/?json&panda_resource=%s&value=%s&reason=%s&probe=hammercloud'" \
#                         % (self.agis_endpoint, queuename, new_status.upper(), reason)
            comm = "curl -S --cert $X509_USER_PROXY --key $X509_USER_PROXY --cacert $X509_USER_PROXY --capath $X509_CERT_DIR 'https://%s/request/pandaqueuestatus/update/set_probestatus/?json&panda_resource=%s&value=%s&reason=%s&probe=hammercloud'" \
                        % (self.agis_endpoint, queuename, new_status.upper(), reason)
            self._logger.info('Executing: %s' % (comm))
            err, out = commands.getstatusoutput(comm)
            if err != 0:
                self._logger.error('Failed to run command %s: %s' % (comm, out))
            else:
                self._logger.debug('Output of command "%s" : %s' % (comm, out))


    def is_autoexclusion_enabled(self, queuename):
        """return True if autoexclusion is enabled for this queue, False otherwise"""
        try:
            site = Site.objects.get(name=queuename)
            opt = SiteOption.objects.get(option_name='autoexclusion', site=site)
        except (Site.DoesNotExist, SiteOption.DoesNotExist):
            return None
        if opt.option_value == 'enable':
            return True
        else:
            return False

    
    ### BEGIN TEST ###
    def get_template_pattern(self, queuename, pr_type, pr_capability, add_also_mcore_himem=False, pr_hc_suite=None):
        """ construct the pattern with which to searth for functional test patterns"""
        self._logger.debug('get_template_pattern(queuename=%s, pr_type=%s, pr_capability=%s, add_also_mcore_himem=%s, pr_hc_suite=%s)' % (queuename, pr_type, pr_capability, add_also_mcore_himem, pr_hc_suite))

        pr_hc_suite_l = [suite.strip() for suite in pr_hc_suite.split(',')]
        templatepatterns = []
        for suite in pr_hc_suite_l:
            templatepattern = '' 
            if suite is not None and suite in self.ALLOWED_HC_SUITES:
                try:
                    templatepattern = '%s ' % (suite.upper())
                except:  
                    self._logger.error('exception: %s'% (repr(sys.exc_info())))
                    traceback.print_exc()
            else:
                isanaly = self.is_analy(pr_type)
                templatepattern = 'AFT' if isanaly else 'PFT'
                if not add_also_mcore_himem: 
                    if self.is_multicore(pr_capability):
                        templatepattern += '_MCORE'
                    if self.is_highmem(pr_capability):
                        templatepattern += '_HIMEM'
                templatepattern += ' '
        templatepatterns.append(templatepattern)
        self._logger.debug('get_template_pattern templatepatterns=%s' % (templatepatterns))
        return templatepatterns
    ### END TEST ###


    ### BEGIN TEST ###
    def site_has_ft(self, queuename, pr_type, pr_capability, pr_hc_suite=None):
        self._logger.debug('will call get_template_pattern from site_has_ft')
        templatepatterns = self.get_template_pattern(queuename, pr_type, pr_capability, pr_hc_suite=pr_hc_suite)
        ### start using hc_suite (HC-444). Here we may have troubles once hc_suite contains a list of values.
        for templatepattern in templatepatterns:
            templatesites = TemplateSite.objects.filter(site__name=queuename, template__description__contains=templatepattern)
            self._logger.debug('called get_template_pattern from site_has_ft, templatesites=%s' % (templatesites))
            if len(templatesites) == 0:
                self._loger.warning('there is no functional test for template pattern %s' %(templatepattern))
                return False
        else:
            return True 
    ### END TEST ###


    ### BEGIN TEST ###
    def add_site_to_ft(self, queuename, pr_type, pr_capability, pr_site, add_also_mcore_himem=False, pr_hc_suite=None):
        """add a queue to its corresponding functional test"""
        ### the DB presence check is not needed, because this method is called only for sites in HC DB
#         try:
#             site = Site.objects.get(name=queuename)
#         except Site.DoesNotExist:
#             self._logger.error('Site %s is not in DB' % (queuename))
#             return


        ### lookup AFT/PFT templatepatterns only for non-MCORE/non-HIMEM resources ... add_also_mcore_himem=False
        ### lookup AFT/PFT templatepatterns also for MCORE/HIMEM ... , add_also_mcore_himem=True
        self._logger.debug('will call get_template_pattern from add_site_to_ft')
        templatepatterns = self.get_template_pattern(queuename, pr_type, pr_capability, add_also_mcore_himem, pr_hc_suite)

        templates_l = []
        for templatepattern in templatepatterns:
            ### add site only to active template. HC-623
            templates = Template.objects.filter(active=True, description__contains=templatepattern)
            templates_l.append(templates)

        merged_templates = reduce(lambda x,y: x | y, templates_l)
        self._logger.debug('called get_template_pattern from add_site_to_ft, merged templates=%s' % (templates))
        for template in merged_templates:
            try:
                ts = TemplateSite.objects.get(site=pr_site, template=template)
            except TemplateSite.DoesNotExist:
                ts = TemplateSite(site=pr_site,
                               template=template,
                               resubmit_enabled=True,
                               resubmit_force=False,
                               num_datasets_per_bulk=1,
                               min_queue_depth=0,
                               max_running_jobs=1)
                if not simulation:
                    ts.save()
                    self._logger.debug('TemplateSite %s saved' % (ts))
                    self.template_changes.append('ADDED: ' + str(ts))
            else:
                pass
    ### END TEST ###


    ### BEGIN TEST ###
    def remove_site_from_ft(self, queuename, pr_type, pr_capability, add_also_mcore_himem=False, pr_hc_suite=None):
        ### the DB presence check is no longer needed, because this method is called only for sites in HC DB
#         try:
#             site = Site.objects.get(name=queuename)
#         except Site.DoesNotExist:
#             self._logger.error('Site %s is not in HC DB, cannot remove it from FT.' % (queuename))
#             return
        self._logger.debug('will call get_template_pattern from remove_site_from_ft')
        templatepatterns = self.get_template_pattern(queuename, pr_type, pr_capability, add_also_mcore_himem, pr_hc_suite)
        templatesites_l = []
        for templatepattern in templatepatterns:
            templatesites = TemplateSite.objects.filter(site__name=queuename, template__description__contains=templatepattern)
            templatesites_l.append( templatesites )

        merged_templatesites = reduce(lambda x,y: x | y, templatesites_l)
        self._logger.debug('called get_template_pattern from remove_site_from_ft, merged templatesites=%s' % (merged_templatesites))

        for ts in merged_templatesites:
            self._logger.warning('Will remove TemplateSite %s' % (ts))
            self.template_changes.append('REMOVED: ' + str(ts))
            if not simulation:
                ts.delete()
    ### END TEST ###



    def remove_site_from_AFT(self, queuename, pr_type, pr_capability, add_also_mcore_himem=False, pr_hc_suite=None):
        ### do not remove category='special' from AFT. HC-604
#         if 'analysis' not in pr_type:
        if pr_hc_suite != 'AFT':
            self.remove_site_from_ft(queuename, 'analysis', pr_capability, add_also_mcore_himem)


    def remove_site_from_PFT(self, queuename, pr_type, pr_capability, add_also_mcore_himem=False, pr_hc_suite=None):
        ### do not remove category='special' from PFT. HC-604
#         if 'production' not in pr_type:
        if pr_hc_suite not in ('PFT', 'PFT_MCORE'):
            self.remove_site_from_ft(queuename, 'production', pr_capability, add_also_mcore_himem)


    def is_multicore(self, pr_capability):
        try:
            return "mcore" in pr_capability
        except TypeError:
            return False

    def is_analy(self, pr_type):
        return "analysis" in pr_type

    def is_standalone(self, pr_is_default):
        return pr_is_default


    def is_highmem(self, pr_capability):
        try:
            return "himem" in pr_capability
        except TypeError:
            return False


    def update_panda_status(self):
        for r in sorted(self.pandaresources, key = lambda x: x['name']):
#             res = self.pandaresources[r]
            res = r
            try:
#                 site = Site.objects.get(name = r)
                site = Site.objects.get(name = res['name'])
                self._logger.debug('Site %s panda_status %s' % (site,res['queue_status'] ))
            except Site.DoesNotExist:
                pass

    def update_ddm(self, queuename, old_ddm_val, new_ddm_val):
        new_ddm = sorted(list(set(new_ddm_val.split(','))))
        old_ddm = sorted(list(set(old_ddm_val.split(','))))
        if new_ddm != old_ddm:
            self._logger.info('Changing ddm of %s from %s' % (queuename, old_ddm))
            self._logger.info('Changing ddm of %s to   %s' % (queuename, new_ddm))
            qs = Site.objects.filter(name=queuename)
            qs.update(ddm=','.join(new_ddm))
            qs[0].save(update_fields=['ddm'],force_update=True)
            #### the instance data does not save into DB unless this functions crashes.
            #### When the crash is caught in a try - except block, the ddm field is not saved in DB.
            #### Found out that saving ddm field in self.update_properties() helps.


    def is_enabled(self, queuename):
        try:
            return Site.objects.get(name=queuename).enabled
        except Site.DoesNotExist:
            return False


    def is_site(self, queuename):
        site=None
        try:
            site=Site.objects.get(name=queuename)
            return (True, site)
        except Site.DoesNotExist:
            return (False, site)


    def mail_new_sites(self, sites):
        if len(sites):
            to = 'hammercloud-notifications-atlas@cern.ch'
            lines = ['The following new sites have been added to the database:','']
            try:
                site_names = [x['name'] for x in sites]
                for s in Site.objects.filter(name__in = site_names):
                    self._logger.debug('New site: %s' % (repr(s)))
                    lines.append("%(sitename)s to cloud %(cloud)s with ddm %(ddm)s : %(link)s" % {'sitename': s.name,
                                                                                                  'cloud': s.cloud,
                                                                                                  'ddm': s.ddm,
                                                                                                  'link': s.monitoring_link})
                    self._logger.debug('continued: New site: [%s]' % (str(lines[-1])))
                body = '\n'.join(lines)
                subject = '[HC-UPDATE-FROM-AGIS] New sites added to DB at ' + str(datetime.datetime.now())
                cernmail.send(to, subject, body)
            except:
                self._logger.error('error sending notification mail')


    def mail_ddm_changes(self):
        to = 'hammercloud-notifications-atlas@cern.ch'
        lines = ['The following ddm changes were applied:','']
        for sitename, old_ddm, new_ddm in self.ddm_changes:
            lines.append("%(sitename)s: from %(old_ddm)s " % {'sitename': sitename,
                                                                            'old_ddm': old_ddm,})
            lines.append("%(sitename)s: to   %(new_ddm)s" % {'sitename': sitename,
                                                                            'new_ddm': new_ddm,})
        body = '\n'.join(lines)
        subject = '[HC-UPDATE-FROM-AGIS] DDM changes applied to DB at ' + str(datetime.datetime.now())
        cernmail.send(to, subject, body)


    def mail_autoexclusion_changes(self):
        to = 'hammercloud-notifications-atlas@cern.ch'
        lines = ['The following autoexclusion changes were applied:','']
        for sitename, new_state in self.autoexclusion_changes:
            try:
                pr_full = [x for x in self.pandaresources if x['name']==sitename]
                site_hc_param = pr_full['hc_param']
                site_capability = pr_full['capability']
                site_type = pr_full['category']
                site_is_default = pr_full['is_default']
                lines.append("%(sitename)s was set to %(new_state)s\t[type=%(type)s, hc_param=%(hc_param)s, capability=%(capability)s, is_default=%(is_default)s]" \
                             % {'sitename': sitename, \
                                'new_state': new_state, \
                                'type': site_type, \
                                'hc_param': site_hc_param, \
                                'capability': site_capability, \
                                'is_default': site_is_default \
                            })
            except:
                lines.append("%(sitename)s was set to %(new_state)s" \
                             % {'sitename': sitename, \
                                'new_state': new_state\
                            })
        body = '\n'.join(lines)
        subject = '[HC-UPDATE-FROM-AGIS] Autoexclusion changes applied at ' + str(datetime.datetime.now())
        cernmail.send(to, subject, body)


    def mail_template_changes(self):
        to = 'hammercloud-notifications-atlas@cern.ch'
        lines = ['The following template changes were applied:','']
        for line in self.template_changes:
            lines.append(line)
        body = '\n'.join(lines)
        subject = '[HC-UPDATE-FROM-AGIS] Template changes applied to DB at ' + str(datetime.datetime.now())
        cernmail.send(to, subject, body)


    def disable_defunct_sites(self):
        panda_resources = sorted(i['name'] for i in self.pandaresources)
        ### re-evaluate enabled_hc_sites. HC-305 HC-126
#         enabled_hc_sites = sorted(i.name for i in Site.objects.filter(enabled=True))
#         enabled_hc_sites = sorted(i['name'] for i in self.pandaresources if i['hc_param'] not in ['False'])
        enabled_hc_sites = sorted(i['name'] for i in self.pandaresources if i['hc_param'] in ['OnlyTest', 'AutoExclusion', 'OnlyExclusion'])
        enabled_in_site_table = sorted(i.name for i in Site.objects.filter(enabled=True))
        ### disable Sites of queues DISABLED in AGIS. HC-465
        defunct_sites = [i for i in enabled_in_site_table if i not in panda_resources]
        for site_name in defunct_sites:
            site = Site.objects.get(name = site_name)
            if not simulation:
                site.enabled = False
                site.save()
            self._logger.warning('site %s disabled, because its queue is DISABLED in/disappeared from AGIS' % (site))
        ### enable sites that were DISABLED long time, but now are ACTIVE. HC-693
        disabled_in_site_table = sorted(i.name for i in Site.objects.filter(enabled=False))
        for site_name in enabled_hc_sites:
            if site_name in disabled_in_site_table:
                self._logger.info('Will set site %s to enabled=True' % (site_name))
                site = Site.objects.get(name = site_name)
                site.enabled = True
                site.save()
        ### re-evaluate disabled_hc_sites. HC-305 HC-126
#         disabled_hc_sites = sorted((i.name, i.category, i.capability) for i in Site.objects.filter(enabled=False))
        disabled_hc_sites = sorted((i['name'], i['category'], i['capability'], i['hc_suite']) for i in self.pandaresources if i['hc_param'] in ['False'])
        self._logger.debug('type=%s disabled_hc_sites=%s' % (type(disabled_hc_sites), disabled_hc_sites))
        ### disable Sites of queues DISABLED in AGIS, remove them from FT. HC-465
        s_defunct = Site.objects.filter(name__in=defunct_sites)
        defunct_hc_sites = sorted((i.name, i.category, i.capability, i.hc_suite) for i in s_defunct)
        self._logger.debug('type=%s defunct_hc_sites=%s' % (type(defunct_hc_sites), defunct_hc_sites))
        disabled_hc_sites += defunct_hc_sites
        self._logger.debug('type=%s disabled_hc_sites=%s' % (type(disabled_hc_sites), disabled_hc_sites))
        if len(disabled_hc_sites):
            for site_name, pr_type, pr_capability, pr_hc_suite in disabled_hc_sites:
                self.remove_site_from_ft(site_name, pr_type, pr_capability, add_also_mcore_himem=True, pr_hc_suite=pr_hc_suite)


    def update_properties(self, site_for_pr, pr_hcparam, pr_capability, pr_isdefault, pr_type, pr_cloud, pr_ddm, pr_enabled, pr_queue, pr_hcsuite):
        """
        update_properties ... check/update changes in
                                                    hc_param,
                                                    capability,
                                                    is_default,
                                                    type,
                                                    cloud,
                                                    ddm,
                                                    enabled,
                                                    queue,
                                                    hc_suite
        """
        ### check hc_param
        self._logger.debug('%s %s: new %s old %s' % (site_for_pr.name, 'hc_param', pr_hcparam, site_for_pr.hc_param))
        if site_for_pr.hc_param != pr_hcparam:
            site_for_pr.hc_param = pr_hcparam
        ### check hc_suite HC-436
        self._logger.debug('%s %s: new %s old %s' % (site_for_pr.name, 'hc_suite', pr_hcsuite, site_for_pr.hc_suite))
        if site_for_pr.hc_suite != pr_hcsuite:
            site_for_pr.hc_suite = pr_hcsuite
        ### check capability
        self._logger.debug('%s %s: new %s old %s' % (site_for_pr.name, 'capability', pr_capability, site_for_pr.capability))
        if site_for_pr.capability != pr_capability:
            site_for_pr.capability = pr_capability
        ### check is_default
        self._logger.debug('%s %s: new %s old %s' % (site_for_pr.name, 'is_default', pr_isdefault, site_for_pr.is_default))
        if site_for_pr.is_default != pr_isdefault:
            site_for_pr.is_default = pr_isdefault
        ### check type/category
        self._logger.debug('%s %s: new %s old %s' % (site_for_pr.name, 'type', pr_type, site_for_pr.category))
        if site_for_pr.category != pr_type:
            site_for_pr.category = pr_type
            ### if the site changed type and is no longer a 'production' or 'analysis' site, remove it from FT. However, test special resources too, if they want to -- HC-172
            ### test only production or analysis resources. HC-366
            ### do not remove category='special' from AFT/PFT. HC-604
#             if site_for_pr.category not in ('production', 'analysis'):
            if pr_hcsuite not in ('AFT', 'PFT', 'PFT_MCORE'):
                self.remove_site_from_ft(site_for_pr.name, pr_type, pr_capability, pr_hc_suite=pr_hcsuite)
        ### check cloud
        self._logger.debug('%s %s: new %s old %s' % (site_for_pr.name, 'type', pr_cloud, site_for_pr.cloud))
        if site_for_pr.cloud != pr_cloud:
            site_for_pr.cloud = pr_cloud
        ### check ddm
        ###### handle update_ddm here instead of in self.update_ddm()
        if pr_hcparam in ('OnlyTest', 'AutoExclusion') and self.is_enabled(site_for_pr.name):
            self._logger.debug('%s %s: new %s old %s' % (site_for_pr.name, 'ddm', pr_ddm, site_for_pr.ddm))
            ### ddm field in AGIS is now the only source of information, do not hardcode anything. HC-359
#             ### do not set Site.ddm to empty string! It will cause problem to ND sites. HC-295
#             if len(pr_ddm) > self.too_short_rse_list:
#                 if site_for_pr.ddm != pr_ddm:
#                     site_for_pr.ddm = pr_ddm
#             else:
#                 self._logger.info('Will not overwrite Site.ddm value for site %s, because the new value [%s] is too short, i.e. keeping the old value [%s].'\
#                                      %(site_for_pr.name, pr_ddm, site_for_pr.ddm))
        if site_for_pr.ddm != pr_ddm:
            site_for_pr.ddm = pr_ddm
        ### check queue (PanDA queue)
        self._logger.debug('%s %s: new %s old %s' % (site_for_pr.name, 'queue', pr_queue, site_for_pr.queue))
        if site_for_pr.queue != pr_queue:
            site_for_pr.queue = pr_queue
        ### check autoexclusion enabled flag
        self._logger.debug('%s %s: new %s old %s' % (site_for_pr.name, 'enabled', pr_enabled, site_for_pr.enabled))
        if site_for_pr.enabled != pr_enabled:
            try:
                changed = SiteOption.set_option(site_for_pr, 'autoexclusion', pr_enabled)
                if changed:
                    self._logger.info('Changing autoexclusion for site %s to %s' % (site_for_pr.name, pr_enabled))
            except Site.DoesNotExist:
                self._logger.error('Site does not exist in db: %s (type=%s)' % (site_for_pr.name, pr_type))
        ### save changes to DB
        site_for_pr.save()


    def update(self):
        self.new_sites = []
        self.disable_defunct_sites()

        self.pandaresources=sorted(self.pandaresources, key = lambda x: x['name'])
        for pr in self.pandaresources:
            pr_name=pr['name']
            pr_capability = pr['capability']
            pr_type = pr['category']
            pr_is_default = pr['is_default']
#             is_multicore = self.is_multicore(pr_capability)
            is_multicore = pr['is_mcore']
#             is_highmem = self.is_highmem(pr_capability)
            is_highmem = pr['is_himem']
            try:
                agis_state=pr['hc_param']
            except KeyError:
                agis_state = 'False'
            pr_ddm = pr['ddm']
            pr_queue = pr['queue']
            pr_hc_suite = pr['hc_suite']

            site_in_db, site_for_pr = self.is_site(pr_name)

            ### sites with hc_param=False are not added to the HC Site table
            if not site_in_db and agis_state in ('OnlyTest', 'AutoExclusion', 'OnlyExclude'):
                self._logger.info('Adding site %s : %s ' % (pr_name, str(pr)))
                self.add_site(pr)
                self.new_sites.append(pr)

            ### do not go through set_autoexclusion loop for site which has hc_param=False
            ### they are not added to the HC Site table anyway!
            if site_in_db:
                ### remove site from FT if it should not be tested
                ### do not remove category='special' sites! HC-604
#                 if (agis_state in ('False', 'OnlyExclude') and self.site_has_ft(pr_name, site_for_pr.category, pr_capability)) \
#                     or (site_for_pr.category not in ('production', 'analysis')):
                if agis_state in ('False', 'OnlyExclude') and self.site_has_ft(pr_name, site_for_pr.category, pr_capability):
                    self._logger.info('Removing FT for site %s' % (pr_name))
                    self._logger.debug('Removing FT for site %s : hc_param=%s site_has_ft=%s type=%s hc_suite=%s' % \
                                      (pr_name, agis_state, self.site_has_ft(pr_name, pr_type, pr_capability), site_for_pr.category, site_for_pr.hc_suite))
                    self.remove_site_from_ft(pr_name, pr_type, pr_capability, pr_hc_suite=site_for_pr.hc_suite)
                    self._logger.debug('Removed FT for site %s' % (pr_name))
                ### add site to FT if it should be tested
                ### remove condition to test only sites with flag enabled=True. HC-304
                if agis_state in ('OnlyTest', 'AutoExclusion'):
#                     #### self.update_ddm() is handled as a part of self.update_properties()
#                     self.update_ddm(pr_name, site_for_pr.ddm, pr_ddm)
                    self._logger.info('Adding FT for site %s ' % (pr_name))
#                     ### DO NOT send SCORE AFT/PFT to MCORE/HIMEM sites
#                     if is_multicore or is_highmem:
#                         ### remove MCORE/HIMEM from AFT/PFT
#                         self._logger.info('Removing FT for site %s ' % (pr_name))
#                         self.remove_site_from_ft(pr_name, pr_type, pr_capability, add_also_mcore_himem=True)
                    ### add site to AFT/PFT if it is not MCORE/HIMEM site
                    ### add site to FT only if it is production or analysis. HC-366
                    ### do not remove category='special' from AFT/PFT. HC-604
#                     if pr_type in ('production', 'analysis'):
                    if site_for_pr.hc_suite in ('AFT', 'PFT', 'PFT_MCORE'):
                        ### remove site from alternate AFT/PFT in case it changed type. HC-391
                        if 'analysis' in pr_type:
                            self._logger.info('Site %s is of type %s. Checking if it is registered for PFT, and if it is, will remove it from PFT.' \
                                              % (pr_name, pr_type))
                            self.remove_site_from_PFT(pr_name, pr_type, pr_capability, site_for_pr, pr_hc_suite = site_for_pr.hc_suite)
                            self._logger.info('Check for site %s PFT done.' % (pr_name))
                        if 'production' in pr_type:
                            self._logger.info('Site %s is of type %s. Checking if it is registered for AFT, and if it is, will remove it from AFT.' \
                                              % (pr_name, pr_type))
                            self.remove_site_from_AFT(pr_name, pr_type, pr_capability, site_for_pr, pr_hc_suite = site_for_pr.hc_suite)
                            self._logger.info('Check for site %s AFT done.' % (pr_name))
                        ### add site to relevant AFT/PFT.
                        self._logger.info('Adding FT for site %s' % (pr_name))
                        self._logger.debug('Adding FT for site %s : hc_param=%s pr_type=%s new hc_suite=%s' % (pr_name, agis_state, pr_type, pr_hc_suite))
                        self.add_site_to_ft(pr_name, pr_type, pr_capability, site_for_pr, pr_hc_suite = pr_hc_suite)
                        self._logger.debug('Added FT for site %s' % (pr_name))
#                 ### is the site a master site?
#                 is_standalone=self.is_standalone(pr_is_default)
#                 ### handle the autoexclusion flag
#                 # autoexclusion, ignore for _MCORE sites
#                 ### Warning: do we want to hardcode no autoexclusion for mcore/himem?
#                 if is_multicore or is_highmem:
#                     self._logger.warning('No autoexclusion for mcore/himem resource %s' % (pr_name))
#                     self.set_autoexclusion(pr_name, False, pr_type)
#                     continue
                ### the autoexclusion enabled flag
#                 autoexclusion_enabled = self.is_autoexclusion_enabled(pr_name)
#                 ### disable autoexclusion for some sites
#                 if agis_state in ('False', 'OnlyTest') and (autoexclusion_enabled == None or autoexclusion_enabled):
#                     self._logger.info('Disabling autoexlusion for site %s (hc_param="%s")' % (pr_name, agis_state))
#                     self.set_autoexclusion(pr_name, False, pr_type)
#                     self.autoexclusion_changes.append((pr_name, 'disabled'))
#                 ### enable autoexclusion for some sites
#                 if agis_state in ('AutoExclusion', 'OnlyExclude') and not autoexclusion_enabled:
#                     self._logger.info('Enabling autoexlusion for site %s (hc_param="%s")' % (pr_name, agis_state))
#                     self.set_autoexclusion(pr_name, True, pr_type)
#                     self.autoexclusion_changes.append((pr_name, 'enabled'))
#                 ### record autoexclusion changes
#                 changed_ax = self.set_autoexclusion(pr_name, agis_state in ('AutoExclusion', 'OnlyExclude'), pr_type)
#                 if changed_ax:
#                     self.autoexclusion_changes.append((pr_name, 'enabled'  if agis_state in ('AutoExclusion', 'OnlyExclude') else 'disabled'))
                pr_enabled = 'enabled'  if agis_state in ('AutoExclusion', 'OnlyExclude') else 'disabled'

                ### ddm field in AGIS is now the only source of information, do not hardcode anything. HC-359
#                 ### handle sites which have empty ddm in AGIS, such as ND sites. HC-295
#                 if len(pr_ddm) < self.too_short_rse_list:
#                     id_pr = self.pandaresources.index(pr)
#                     self._logger.debug(':753 pr=%s' % (str(pr)))
#                     self.pandaresources[id_pr]['ddm']=site_for_pr.ddm

                ### check/update changes in hc_param, capability, is_default, type, ddm, queue
                self.update_properties(site_for_pr, pr['hc_param'], pr['capability'], pr['is_default'], pr['category'], pr['cloud'], pr['ddm'], pr_enabled, pr['queue'], pr['hc_suite'])


    def print_mcore_autoexclusion_sites(self):
        resmcore=[x['name'] for x in self.new_sites \
                  if self.is_multicore(x['capability']) \
                  and x['hc_param'] in ('AutoExclusion', 'OnlyExclude') ]
        if len(resmcore):
            self._logger.info('The following MCORE sites want autoexclusion: %s' % (', '.join(resmcore)))


    def dump_json(self, filename, data):
        f=open(filename, "w")
        f.write(simplejson.dumps(data, sort_keys=True, indent=2))
        f.close()


    def serialize_pandaresources_for_json(self):
        """
        serialize_pandaresources_for_json ... serialize self.pandaresources for dump
            because
                TypeError: <Backend: PanDA> is not JSON serializable
            same with Cloud object
        """
        result=[]
        for pr in self.pandaresources:
            pr['backend']=pr['backend'].__str__()
            pr['cloud']=pr['cloud'].__str__()
            result.append(pr)
        return result


    def pandaresource_save_to_file(self, file_pr='/tmp/agis_pandaresources.json', \
                                 filetxt_mcore='/tmp/agis_mcore_sites.txt', \
                                 filetxt_slave='/tmp/agis_slave_sites.txt', \
                                 file_mcore='/tmp/agis_mcore_sites.json', \
                                 file_slave='/tmp/agis_slave_sites.json', \
                                 file_nomaster='/tmp/agis_no-master_sites.json'):
        """
        pandaresource_save_to_file ... save topology info into files
        """
        ### print file names
        self._logger.info('JSON with PanDA resources will be saved in file: %s' % (file_pr))
        self._logger.info('List of mcore sites will be saved in file: %s' % (filetxt_mcore))
        self._logger.info('JSON with mcore sites will be saved in file: %s' % (file_mcore))
        self._logger.info('JSON with slave sites will be saved in file: %s' % (file_slave))
        self._logger.info('JSON with slave sites without master will be saved in file: %s' % (file_nomaster))
        ### prepare for serialization
        data = self.serialize_pandaresources_for_json()
#         simplejson.dump(self.pandaresources, open(file_pr, "w"), sort_keys=True, indent=2)
        self.dump_json(file_pr, data)
        mcore_dict = defaultdict(list)
        slave_dict = defaultdict(list)
        no_master = []

        mcore_file = open(filetxt_mcore, "w")
        slave_file = open(filetxt_slave, "w")
#         for r in sorted(self.pandaresources, key = lambda x: x['name']):
        for r in sorted(data, key = lambda x: x['name']):
            res = r
            if len(res['default_resource']) < 1:
                no_master.append(r['name'])
            if res['is_slave'] and res['queue_status'] in ('online', 'test') and res['hc_param'] in ("AutoExclusion", "OnlyExclude"):
                if res['is_mcore']:
                    mcore_file.write('%s\t%s\n' % (res['default_resource'], r))
                    if len(res['default_resource']):
                        mcore_dict[res['default_resource']].append(r['name'])
                else:
                    slave_file.write('%s\t%s\n' % (res['default_resource'], r))
                    if len(res['default_resource']):
                        slave_dict[res['default_resource']].append(r['name'])
            if res['is_slave'] and res['queue_status'] in  ('online', 'test') and res['hc_param'] in ("AutoExclusion", "OnlyExclude"):
                if res['is_mcore']:
                    mcore_file.write('%s\t%s\n' % (res['default_resource'], r))
                    if len(res['default_resource']):
                        mcore_dict[res['default_resource']].append(r['name'])
                else:
                    slave_file.write('%s\t%s\n' % (res['default_resource'], r))
                    if len(res['default_resource']):
                        slave_dict[res['default_resource']].append(r['name'])
        mcore_file.close()
        slave_file.close()
#         simplejson.dump(mcore_dict, open(file_mcore, "w"), sort_keys=True, indent=2)
#         simplejson.dump(slave_dict, open(file_slave, "w"), sort_keys=True, indent=2)
#         simplejson.dump(no_master, open(file_nomaster, "w"), sort_keys=True, indent=2)
        self.dump_json(file_mcore, mcore_dict)
        self.dump_json(file_slave, slave_dict)
        self.dump_json(file_nomaster, no_master)


    def distribute_outputs(self):
        self._logger.info('Transferring files to ATLAS submission hosts')
        try:
            ### distribute to submit nodes
            hct = filemover.HCTransfers('atlas', load_dest_hosts=True)
            ### add AGIS output files
            for output_file in [self.file_pr, self.filetxt_mcore, self.filetxt_slave, self.file_mcore, self.file_slave, self.file_nomaster]:
                self._logger.info('Registering file for transfer: %s' % (output_file))
                hct.add_file_transfer(output_file, output_file)
            ### transfer
            hct.transfer_files()
            self._logger.info('Transfers complete')

            ### distribute to core node #FIXME - not necessary once update_from_agis4 runs on hammercloud-ai-03
            destination_hosts = ['hammercloud-ai-03.cern.ch']
            hct = filemover.HCTransfers(app='atlas', load_dest_hosts=False, \
                                        destination_hosts=destination_hosts)
            ### add output files
            for output_file in [self.file_pr, self.filetxt_mcore, self.filetxt_slave, self.file_mcore, self.file_slave, self.file_nomaster]:
                self._logger.info('Registering file for transfer: %s' % (output_file))
                hct.add_file_transfer(output_file, output_file)
            ### transfer
            hct.transfer_files()
            self._logger.info('Transfers complete')

        except Exception as e:
            self._logger.error('Transfer failed: %s' % (str(e)))
            traceback.print_exc()


if __name__ == '__main__':
#     if __package__ is None:
#         __package__ = "apps.atlas.python.scripts.server.update_from_agis4"

    try:
        updater = UpdateFromAgis()
        updater.update()
        updater.print_mcore_autoexclusion_sites()
        if updater.new_sites:
            updater.mail_new_sites(updater.new_sites)
        if updater.ddm_changes:
            updater.mail_ddm_changes()
        if updater.autoexclusion_changes:
            updater.mail_autoexclusion_changes()
        if updater.template_changes:
            updater.mail_template_changes()

        updater._logger.debug('### Site\tDDM')
        for r in updater.pandaresources:
            try:
                updater._logger.debug('%40s\t\t%s' % (r['name'], r['ddm']))
            except:
                pass

        ### save topology info into files FIXME
#         updater.pandaresource_save_to_file(
#             file_pr='/tmp/agis_pandaresources.json', \
#             filetxt_mcore='/data/hc/apps/atlas/python/scripts/server/agis_mcore_sites.txt', \
#             filetxt_slave='/data/hc/apps/atlas/python/scripts/server/agis_slave_sites.txt', \
#             file_mcore='/data/hc/apps/atlas/python/scripts/server/agis_mcore_sites.json', \
#             file_slave='/data/hc/apps/atlas/python/scripts/server/agis_slave_sites.json', \
#             file_nomaster='/data/hc/apps/atlas/python/scripts/server/agis_no-master_sites.json'
#             )
        updater.pandaresource_save_to_file(
            file_pr=updater.file_pr, \
            filetxt_mcore=updater.filetxt_mcore, \
            filetxt_slave=updater.filetxt_slave, \
            file_mcore=updater.file_mcore, \
            file_slave=updater.file_slave, \
            file_nomaster=updater.file_nomaster
            )
# #         updater.pandaresource_save_to_file(
# #             file_pr='/tmp/agis_pandaresources.json', \
# #             filetxt_mcore='/data/jschovan/hc/apps/atlas/python/scripts/server/agis_mcore_sites.txt', \
# #             filetxt_slave='/data/jschovan/hc/apps/atlas/python/scripts/server/agis_slave_sites.txt', \
# #             file_mcore='/data/jschovan/hc/apps/atlas/python/scripts/server/agis_mcore_sites.json', \
# #             file_slave='/data/jschovan/hc/apps/atlas/python/scripts/server/agis_slave_sites.json', \
# #             file_nomaster='/data/jschovan/hc/apps/atlas/python/scripts/server/agis_no-master_sites.json'
# #             )
        ### distribute outputs to submit nodes:
        updater.distribute_outputs()


    except Exception:
        mail_failure()
    else:
        updater._logger.info('All succeded - Over and out')
#TemplateSite.objects.filter(site__name='ANALY_LRZ', template__description__contains='AFT ')
