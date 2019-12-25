# pyppl_strict

More strict check of job success for PyPPL

## Features:

1. Checking all output files to make sure they are generated
2. allowing custom returncode settings
3. allowing a custom script to check the output file

## Usage

```python
# allow returncode 1 to be valid as well
# (0 is valid of couse)
PyPPL(config_strict_rc = 1).start(...).run()
```

The if your job returns `1`, it won't fail.

```python
p = Proc(config_strict_expect = 'grep 123 {{o.outfile}}')
p.input = {'a': [1]}
p.output = 'outfile:file:{{i.a}}.txt'
p.script = 'echo 124 > {{o.outfile}}

PyPPL().start(p).run()

# will fail

# following script will pass
p.script = 'echo 123 > {{o.outfile}}'
```
