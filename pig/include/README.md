# Pig Macro Repository

This directory contains useful Pig macros.


## Usage

Macros must be imported using the `IMPORT` keyword and the HDFS path to the file containing the macro. 
If your job is running through Oozie, you can add `${krakenLibPath}/pig` to `oozie.libpath` to enable a relative import:

```pig
IMPORT 'include/<name>.pig';
```

But an absolute path will always do:

```pig
IMPORT 'hdfs:///libs/kraken/pig/include/<name>.pig';
```

