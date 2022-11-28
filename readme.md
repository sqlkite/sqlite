A Go SQLite driver which exposes the exact functionality we needed, exactly how we need it.

Inspired by [go-sqlite-lite](https://github.com/bvinc/go-sqlite-lite).

## Upgrading SQLite

Grab the "Alternative Source Code Formats" from https://www.sqlite.org/download.html, build and copy the sqlite3.c and sqlite3.h files to this repo:

```
wget https://www.sqlite.org/2022/sqlite-src-3390200.zip
unzip sqlite-src-3390200.zip 
cd sqlite-src-3390200
CFLAGS='-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1' ./configure
make sqlite3.c
cp sqlite3.c sqlite3.h PATH_TO_THIS_REPO/
```
