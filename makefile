all: pgaccounts

pgaccounts: pgaccounts.o strbuf.o
	gcc -o pgaccounts pgaccounts.o strbuf.o -lpq

pgaccounts.o: pgaccounts.c
	gcc -c -g -Wall -o $@ $<

strbuf.o: strbuf.c
	gcc -c -g -Wall -o $@ $<

clean:
	-rm pgaccounts
	-rm *.o
