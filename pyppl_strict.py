"""More strict check of job success for PyPPL
Features:
1. make sure all outputs have been generated
2. allow custom returncode settings
3. allow a custom script to check the output file
"""
import cmdy
from os import utime
from pyppl.plugin import hookimpl
from pyppl.jobmgr import STATES
from pyppl.utils import always_list, fs
from pyppl._proc import OUT_VARTYPE

__version__ = "0.0.2"

RC_NO_OUTFILE  = 5000
RC_EXPECT_FAIL = 10000

def strict_rc_converter(rc):
	if not rc:
		return [0]
	if isinstance(rc, str):
		rc = always_list(rc)
	rc = list(rc)
	if 0 not in rc:
		rc.insert(0, 0)
	return rc

def show_error(job, total):
	if job.rc > RC_EXPECT_FAIL:
		msg = '%s (Expectation failed)' % (job.rc - RC_EXPECT_FAIL)
	elif job.rc > RC_NO_OUTFILE:
		msg = '%s (Output file/dir not generated)' % (job.rc - RC_NO_OUTFILE)
	else:
		msg = '%s (Script failed)' % job.rc

	if job.proc.errhow == 'ignore':
		job.logger(
			'Failed but ignored (totally {total}). Return code: {msg}.'.format(
				total = total, msg = msg), level = 'warning')
		return

	job.logger('Failed (totally {total}). Return code: {msg}.'.format(
		total = total, msg = msg), level = 'failed')

	job.logger('Script: {}'.format(job.dir / 'job.script'), level = 'failed')
	job.logger('Stdout: {}'.format(job.dir / 'job.stdout'), level = 'failed')
	job.logger('Stderr: {}'.format(job.dir / 'job.stderr'), level = 'failed')

	# errors are not echoed, echo them out
	if job.index not in job.proc.config.get('echo_jobs', []) or \
		'stderr' not in job.proc.config.get('echo_types', {}):

		job.logger('Check STDERR below:', level = 'failed')
		errmsgs = []
		if job.dir.joinpath('job.stderr').exists():
			errmsgs = job.dir.joinpath('job.stderr').read_text().splitlines()

		if not errmsgs:
			errmsgs = ['<EMPTY STDERR>']

		for errmsg in errmsgs[-20:] if len(errmsgs) > 20 else errmsgs:
			job.logger(errmsg, level = 'failed')

		if len(errmsgs) > 20:
			job.logger(
				'[ Top {top} line(s) ignored, see all in stderr file. ]'.format(
					top = len(errmsgs) - 20), level = 'failed')

@hookimpl
def logger_init(logger):
	logger.add_level('FAILED', 'ERROR')
	logger.add_sublevel('OUTFILE_NOT_EXISTS', -1)
	logger.add_sublevel('EXPECTATION_FAILED', -1)

@hookimpl
def setup(config):
	config.config.strict_rc = [0]
	config.config.strict_expect = ""

@hookimpl
def proc_init(proc):
	def strict_expect_converter(expect):
		return proc.template(expect, **proc.envs)
	proc.add_config('strict_rc', default = 0, converter = strict_rc_converter)
	proc.add_config('strict_expect', default = '', converter = strict_expect_converter)

@hookimpl
def job_succeeded(job):
	if job.rc not in job.proc.config.strict_rc:
		return False

	# check if all outputs are generated
	# refresh stat
	utime(job.dir.joinpath('output'), None)
	for outtype, outdata in job.output.values():
		if outtype not in OUT_VARTYPE and not fs.exists(outdata):
			job.rc += RC_NO_OUTFILE
			job.logger('Outfile not generated: {}'.format(outdata),
				dlevel = "OUTFILE_NOT_EXISTS", level = 'debug')
			return False

	expect_cmd = job.proc.config.strict_expect.render(job.data)
	if expect_cmd:
		cmd = cmdy.bash(c = expect_cmd, _raise = False) # pylint: disable=no-member
		if cmd.rc != 0:
			job.rc += RC_EXPECT_FAIL
			job.logger(expect_cmd, dlevel = "EXPECTATION_FAILED", level = 'error')
			return False
	return True

@hookimpl
def job_build(job, status):
	if status == 'failed':
		show_error(job,
			len([fjob for fjob in job.proc.jobs
				if fjob.state == STATES.BUILTFAILED]))

@hookimpl
def proc_postrun(proc, status):
	if status == 'failed':
		failed_jobs = [job for job in proc.jobs if job.state in (
			STATES.ENDFAILED, STATES.DONEFAILED,
			STATES.SUBMITFAILED, STATES.BUILTFAILED
		)]
		show_error(random.choice(failed_jobs), total = len(failed_jobs))
