CC=gcc
CPPFLAGS+=-I/opt/circonus/include
LDFLAGS+=-L/opt/circonus/lib
LIBS+=-lfq -lcurl
OS=$(shell uname)

ifeq ($(OS),SunOS)
CFLAGS+=-m64
CPPFLAGS := -I/opt/circonus/include/amd64 -I/usr/include/amd64 $(CPPFLAGS)
LDFLAGS := -L/opt/circonus/lib/amd64 -R/opt/circonus/lib/amd64 -m64 $(LDFLAGS)
else
ifeq ($(OS),Darwin)
else
ifeq ($(OS),Linux)
LDFLAGS+=-Wl,-rpath=/opt/circonus/lib
endif
endif
endif


all:	fq2jaeger

OBJS=main.o

.c.o:
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

fq2jaeger:	$(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS) $(LIBS)

clean:
	rm -f *.o fq2jaeger
