"""More strict check of job success for PyPPL
Features:
1. make sure all outputs have been generated
2. allow custom returncode settings
3. allow a custom script to check the output file
"""
from os import utime
import random
import cmdy
from pyppl.plugin import hookimpl
from pyppl.config import config
from pyppl.jobmgr import STATES
from pyppl.utils import always_list, fs, filesig
from pyppl._proc import OUT_VARTYPE
from pyppl._job import RC_NO_RCFILE

__version__ = "0.0.7"

RC_NO_OUTFILE = 5000
RC_EXPECT_FAIL = 10000

config.config.strict_rc = [0]
config.config.strict_expect = ""

def strict_rc_converter(rc):
    """Convert return code from input"""
    if not rc:
        return [0]
    if isinstance(rc, str):
        rc = always_list(rc)
    rc = list(rc)
    if 0 not in rc:
        rc.insert(0, 0)
    return rc


def show_error(job, total):
    """Show error message for a job"""
    if job.rc >= RC_EXPECT_FAIL:
        msg = '%s (Expectation failed)' % (job.rc - RC_EXPECT_FAIL)
    elif job.rc >= RC_NO_OUTFILE:
        msg = '%s (Output file/dir not generated)' % (job.rc - RC_NO_OUTFILE)
    elif job.rc == RC_NO_RCFILE:
        msg = '- (No RC file generated)'
    else:
        msg = '%s (Script failed)' % job.rc

    if job.proc.errhow == 'ignore':
        job.logger(
            f'Failed but ignored (totally {total}). Return code: {msg}.',
            level='warning',
            plugin='strict'
        )
        return

    job.logger(f'Failed (totally {total}). Return code: {msg}.',
               level='failed',
               plugin='strict')

    job.logger(f'Script: {job.dir / "job.script"}',
               level='failed', plugin='strict')
    job.logger(f'Stdout: {job.dir / "job.stdout"}',
               level='failed', plugin='strict')
    job.logger(f'Stderr: {job.dir / "job.stderr"}',
               level='failed', plugin='strict')

    # errors are not echoed, echo them out
    if (job.index not in job.proc.config.get('echo_jobs', [])
            or 'stderr' not in job.proc.config.get('echo_types', {})):

        job.logger('Check STDERR below:', level='failed', plugin='strict')
        errmsgs = []
        if job.dir.joinpath('job.stderr').exists():
            errmsgs = job.dir.joinpath('job.stderr').read_text().splitlines()

        if not errmsgs:
            errmsgs = ['<EMPTY STDERR>']

        for errmsg in errmsgs[-20:] if len(errmsgs) > 20 else errmsgs:
            job.logger(errmsg, level='failed', plugin='strict')

        if len(errmsgs) > 20:
            job.logger(
                '[ Top {top} line(s) ignored, see all in stderr file. ]'.
                format(top=len(errmsgs) - 20),
                level='failed', plugin='strict'
            )


@hookimpl
def logger_init(logger):
    """Add log levels"""
    logger.add_level('FAILED', 'ERROR')
    logger.add_sublevel('OUTFILE_NOT_EXISTS', -1)
    logger.add_sublevel('EXPECTATION_FAILED', -1)



@hookimpl
def proc_init(proc):
    """Add configs"""
    def strict_expect_converter(expect):
        if isinstance(expect, proc.template):
            return expect
        return proc.template(expect, **proc.envs)

    proc.add_config('strict_rc', default=0, converter=strict_rc_converter)
    proc.add_config('strict_expect',
                    default='',
                    converter=strict_expect_converter)


@hookimpl
def job_succeeded(job):
    """Check rc, expect and outfiles to tell if a job is really succeeded"""
    if job.rc not in job.proc.config.strict_rc:
        return False

    # check if all outputs are generated
    # refresh stat
    outdir, mtime = filesig(job.dir.joinpath('output'), job.proc.dirsig)
    utime(outdir, (mtime, mtime))
    for outtype, outdata in job.output.values():
        if outtype not in OUT_VARTYPE and not fs.exists(outdata):
            job.rc += RC_NO_OUTFILE
            job.logger('Outfile not generated: {}'.format(outdata),
                       slevel="OUTFILE_NOT_EXISTS",
                       level='debug',
                       plugin='strict')
            return False

    expect_cmd = job.proc.config.strict_expect.render(job.data)
    if expect_cmd:
        cmd = cmdy.bash(c=expect_cmd, _raise=False)  # pylint: disable=no-member
        if cmd.rc != 0:
            job.rc += RC_EXPECT_FAIL
            job.logger(expect_cmd,
                       slevel="EXPECTATION_FAILED",
                       level='error',
                       plugin='strict')
            return False
    return True


@hookimpl
def proc_postrun(proc, status):
    """Show error message for failed jobs"""
    if status == 'failed':
        failed_jobs = [
            job for job in proc.jobs
            if job.state in (STATES.ENDFAILED, STATES.DONEFAILED,
                             STATES.SUBMITFAILED, STATES.BUILTFAILED,
                             STATES.KILLED, STATES.KILLFAILED)
        ]
        failed_jobs = failed_jobs or [proc.jobs[0]]
        show_error(random.choice(failed_jobs), total=len(failed_jobs))
