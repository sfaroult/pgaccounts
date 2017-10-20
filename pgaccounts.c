/*
 *      pgaccounts.c
 *
 *      Manage student postgresql accounts from a CSV roster
 *
 *      Written by S. Faroult, 2017
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pwd.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <libpq-fe.h>

#include "strbuf.h"

#define  DEF_SEP    ','
#define  DEF_ID      (short)1

#define OPTIONS    "h:CDi:p:r:s:U:x:d:"

#define NO_OP       (short)0
#define CREATE      (short)1
#define DELETE      (short)2

#define STR_CNT     (short)4
#define STR_HOST    (short)0
#define STR_USER    (short)1
#define STR_ROLE    (short)2
#define STR_DB      (short)3

#define CONNECT_STR_LEN  500
#define ID_LEN           250

static char     G_sep = DEF_SEP;
static short    G_id = DEF_ID; 
static short    G_pwd = DEF_ID; 
static PGconn  *G_conn = NULL;
static char    *G_str[STR_CNT] = {NULL, NULL, NULL, NULL};
static short    G_warnings = 0;

// We ignore warnings, we just count them
void ignore_notice(void *arg, const PGresult *res) {
    G_warnings++;
}

static void new_savepoint(void) {
    PGresult    *res;

    if (G_conn) {
      res = PQexec(G_conn, "savepoint sv");
      if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "savepoint: %s\n", 
                        PQresultErrorMessage((const PGresult *)res));
      }
      PQclear(res);
    }
}

static void remove_savepoint(char rollback) {
    PGresult    *res;

    if (G_conn) {
      if (rollback) {
        res = PQexec(G_conn, "rollback to savepoint sv");
        if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
          fprintf(stderr, "rollback savepoint: %s\n", 
                        PQresultErrorMessage((const PGresult *)res));
        }
        PQclear(res);
      }
      res = PQexec(G_conn, "release savepoint sv");
      if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "release savepoint: %s\n", 
                        PQresultErrorMessage((const PGresult *)res));
      }
      PQclear(res);
    }
}

static void start_tx(void) {
    PGresult    *res;

    if (G_conn) {
      res = PQexec(G_conn, "start transaction");
      if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "start tx: %s\n", 
                        PQresultErrorMessage((const PGresult *)res));
      }
      PQclear(res);
    }
}

static void commit_tx(void) {
    PGresult    *res;

    if (G_conn) {
      res = PQexec(G_conn, "commit");
      if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "commit tx: %s\n", 
                        PQresultErrorMessage((const PGresult *)res));
      }
      PQclear(res);
    }
}

static void rollback_tx(void) {
    PGresult    *res;

    if (G_conn) {
      res = PQexec(G_conn, "rollback");
      if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "rollback tx: %s\n", 
                        PQresultErrorMessage((const PGresult *)res));
      }
      PQclear(res);
    }
}

static char create_account(char *id, char *pwd, char *role) {
    PGresult    *res;
    STRBUF       cmd;
    char         ok = 1;

    if (G_conn && id && pwd) {
      strbuf_init(&cmd);
      start_tx();
      // create user <id> with password <pwd> 
      strbuf_add(&cmd, "create user ");
      if (isdigit(*id)) {
        strbuf_addc(&cmd, 'u');
      }
      strbuf_add(&cmd, id);
      strbuf_add(&cmd, " with password '");
      strbuf_add(&cmd, pwd);
      strbuf_addc(&cmd, '\'');
      res = PQexec(G_conn, (const char *)cmd.s);
      if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "create user: %s\n", 
                        PQresultErrorMessage((const PGresult *)res));
        ok = 0;
      }
      PQclear(res);
      strbuf_clear(&cmd);
      if (ok) {
        // create schema <id> authorization <id>
        strbuf_add(&cmd, "create schema ");
        if (isdigit(*id)) {
          strbuf_addc(&cmd, 's');
        }
        strbuf_add(&cmd, id);
        strbuf_add(&cmd, " authorization ");
        if (isdigit(*id)) {
          strbuf_addc(&cmd, 'u');
        }
        strbuf_add(&cmd, id);
        res = PQexec(G_conn, (const char *)cmd.s);
        if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
          fprintf(stderr, "create schema: %s\n", 
                          PQresultErrorMessage((const PGresult *)res));
          ok = 0;
        }
        PQclear(res);
        strbuf_clear(&cmd);
      }
      if (ok) {
        strbuf_add(&cmd, "alter role ");
        if (isdigit(*id)) {
          strbuf_addc(&cmd, 'u');
        }
        strbuf_add(&cmd, id);
        strbuf_add(&cmd, " set search_path to ");
        if (isdigit(*id)) {
          strbuf_addc(&cmd, 's');
        }
        strbuf_add(&cmd, id);
        strbuf_add(&cmd, ",public");
        res = PQexec(G_conn, (const char *)cmd.s);
        if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
          fprintf(stderr, "grant all on schema: %s\n", 
                          PQresultErrorMessage((const PGresult *)res));
          ok = 0;
        }
        PQclear(res);
        strbuf_clear(&cmd);
      }
      if (ok && role) { 
        // grant <role> to <id>
        strbuf_add(&cmd, "grant ");
        strbuf_add(&cmd, role);
        strbuf_add(&cmd, " to ");
        if (isdigit(*id)) {
          strbuf_addc(&cmd, 'u');
        }
        strbuf_add(&cmd, id);
        res = PQexec(G_conn, (const char *)cmd.s);
        if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
          fprintf(stderr, "grant role: %s\n", 
                          PQresultErrorMessage((const PGresult *)res));
          ok = 0;
        }
        PQclear(res);
      }
      if (ok) {
        commit_tx();
      } else {
        rollback_tx();
      }
      strbuf_dispose(&cmd);
    }
    return ok;
}

static char delete_account(char *id) {
    PGresult    *res;
    STRBUF       cmd;
    char         ok = 1;
    int          status;
    int          warnings;

    if (G_conn && id) {
      start_tx();
      strbuf_init(&cmd);
      strbuf_add(&cmd, "drop schema if exists ");
      if (isdigit(*id)) {
        strbuf_addc(&cmd, 'S');
      }
      strbuf_add(&cmd, id);
      strbuf_add(&cmd, " cascade");
      res = PQexec(G_conn, (const char *)cmd.s);
      if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "drop schema: %s\n", 
                        PQresultErrorMessage((const PGresult *)res));
        ok = 0;
      }
      PQclear(res);
      strbuf_clear(&cmd);
      strbuf_init(&cmd);
      strbuf_add(&cmd, "drop owned by ");
      if (isdigit(*id)) {
        strbuf_addc(&cmd, 'U');
      }
      strbuf_add(&cmd, id);
      strbuf_add(&cmd, " restrict");
      // Ignore errors (if objects remain, the following "drop user"
      // will fail)
      new_savepoint();
      res = PQexec(G_conn, (const char *)cmd.s);
      status = PQresultStatus((const PGresult *)res);
      remove_savepoint((status != PGRES_COMMAND_OK));
      PQclear(res);
      strbuf_clear(&cmd);
      if (ok) {
        strbuf_add(&cmd, "drop user if exists ");
        if (isdigit(*id)) {
          strbuf_addc(&cmd, 'U');
        }
        strbuf_add(&cmd, id);
        warnings = G_warnings;
        res = PQexec(G_conn, (const char *)cmd.s);
        if (PQresultStatus((const PGresult *)res) != PGRES_COMMAND_OK) {
          fprintf(stderr, "drop user: %s\n", 
                          PQresultErrorMessage((const PGresult *)res));
          ok = 0;
        }
        if (G_warnings > warnings) {
          ok = 0;
        }
        PQclear(res);
        strbuf_clear(&cmd);
      }
      strbuf_dispose(&cmd);
      if (ok) {
        commit_tx();
      } else {
        rollback_tx();
      }
    }
    return ok;
}

static void init_strings(void) {
    short i;
    for (i = 0; i < STR_CNT; i++) {
      G_str[i] = NULL;
    }
}

static void free_strings(void) {
    short i;
    // fprintf(stderr, "free_strings"); fflush(stderr);
    for (i = 0; i < STR_CNT; i++) {
      if (G_str[i] != NULL) {
        free(G_str[i]);
        G_str[i] = NULL;
      }
    }
    // fprintf(stderr, " ... done\n"); fflush(stderr);
}

static void disconnect() {
    // fprintf(stderr, "disconnect"); fflush(stderr);
    if (G_conn) {
      PQfinish(G_conn);
      G_conn = NULL;
    }
    // fprintf(stderr, " ... done\n"); fflush(stderr);
}

static void connect() {
    char  cnx[CONNECT_STR_LEN];
    short i;
    char  *p;

    strncpy(cnx, "dbname=", CONNECT_STR_LEN);
    strncat(cnx, G_str[STR_DB], CONNECT_STR_LEN - strlen(cnx));
    for (i = 0; i < STR_CNT; i++) {
      if (G_str[i]) {
        switch(i) {
          case STR_HOST:
               if ((p = strchr(G_str[i], ':')) != NULL) {
                 *p++ = '\0';
                 strncat(cnx, " port=", CONNECT_STR_LEN - strlen(cnx));
                 strncat(cnx, p, CONNECT_STR_LEN - strlen(cnx));
               }
               strncat(cnx, " host=", CONNECT_STR_LEN - strlen(cnx));
               strncat(cnx, G_str[i], CONNECT_STR_LEN - strlen(cnx));
               break;
          case STR_USER:
               strncat(cnx, " user=", CONNECT_STR_LEN - strlen(cnx));
               strncat(cnx, G_str[i], CONNECT_STR_LEN - strlen(cnx));
               strncat(cnx, " password=", CONNECT_STR_LEN - strlen(cnx));
               strncat(cnx, getpass("Password: "),
                       CONNECT_STR_LEN - strlen(cnx));
               break;
          default:
               break;
        }
      }
    }
    G_conn = PQconnectdb((const char *)cnx);
    if (PQstatus(G_conn) != CONNECTION_OK) {
      fprintf(stderr, "Connection to database failed:\n%s",
                      PQerrorMessage(G_conn));
      exit(1);
    }
    atexit(disconnect);
}

static void usage(FILE* fp, char *prog) {
  fprintf(fp, "Usage: %s [operation flag] [flags] <CSV roster>\n", prog);
  fprintf(fp, "  Operation flags:\n");
  fprintf(fp, "    -C        : Create accounts\n");
  fprintf(fp, "    -D        : Delete accounts\n");
  fprintf(fp, "  Flags:\n");
  fprintf(fp, "    -?        : Display this\n");
  fprintf(fp,
          "    -h <host> : Postgres server (optionally followed by ':port')\n");
  fprintf(fp, "    -U <name> : Postgres superuser\n");
  fprintf(fp, "    -i <n>    : Identifier is field <n> (default %hd)\n",
         DEF_ID);
  fprintf(fp, "    -p <n>    : Initial password is field <n> (default %hd)\n",
         DEF_ID);
  fprintf(fp, "    -r <role> : Grant <role> to account\n");
  fprintf(fp, "    -s <c>    : Set file separator to <c> (default '%c')\n\n",
         DEF_SEP);
  fprintf(fp,
          "    -x <n>    : Omit the first <n> lines in the roster (headers)\n");
}

int main(int argc, char **argv) {
    int          ch;
    short        op = NO_OP;
    struct stat  buf;
    short        optnum = 0;
    char        *prog = argv[0];
    char         account[ID_LEN];
    short        aidx = 0;
    char         defpass[ID_LEN];
    short        pidx = 0;
    short        linecnt = 0;
    short        skip = 0;
    short        fieldnum = 1;
    FILE        *fp = NULL;
    char         blind = 0;
    short        cnt = 0;

    init_strings();
    atexit(free_strings);
    // The operation flag is forced to come first
    // to emphasize its importance
    while ((ch = getopt(argc, argv, OPTIONS)) != -1) {
      optnum++;
      switch (ch) {
        case 'C':
             if (optnum != 1) {
               if (op != NO_OP) {
                 fprintf(stderr, "Incompatible flags -D and -C.\n");
               } else {
                 fprintf(stderr, "Operation flag must come first.\n");
               }
               usage(stderr, prog);
               exit(1);
             }  
             op = CREATE;
             break;
        case 'D':
             if (optnum != 1) {
               if (op != NO_OP) {
                 fprintf(stderr, "Incompatible flags -C and -D.\n");
               } else {
                 fprintf(stderr, "Operation flag must come first.\n");
               }
               usage(stderr, prog);
               exit(1);
             }
             op = DELETE;
             break;
        case 'i':
             if (optnum == 1) {
               fprintf(stderr, "Operation flag must come first.\n");
               usage(stderr, prog);
               exit(1);
             }
             if (sscanf(optarg, "%hd", &G_id) != 1) {
               fprintf(stderr, "Invalid field number '%s'.\n", optarg);
               usage(stderr, prog);
               exit(1);
             } 
             if (G_id <= 0) {
               fprintf(stderr, "Invalid field number %d.\n", G_id);
               usage(stderr, prog);
               exit(1);
             } 
             break;
        case 'p':
             if (optnum == 1) {
               fprintf(stderr, "Operation flag must come first.\n");
               usage(stderr, prog);
               exit(1);
             }
             if (sscanf(optarg, "%hd", &G_pwd) != 1) {
               fprintf(stderr, "Invalid field number '%s'.\n", optarg);
               usage(stderr, prog);
               exit(1);
             } 
             if (G_pwd <= 0) {
               fprintf(stderr, "Invalid field number %d.\n", G_pwd);
               usage(stderr, prog);
               exit(1);
             } 
             break;
        case 's':
             if (optnum == 1) {
               fprintf(stderr, "Operation flag must come first.\n");
               usage(stderr, prog);
               exit(1);
             }
             G_sep = *optarg;
             break;
        case 'h':
             if (optnum == 1) {
               fprintf(stderr, "Operation flag must come first.\n");
               usage(stderr, prog);
               exit(1);
             }
             G_str[STR_HOST] = strdup(optarg);
             break;
        case 'U':
             if (optnum == 1) {
               fprintf(stderr, "Operation flag must come first.\n");
               usage(stderr, prog);
               exit(1);
             }
             G_str[STR_USER] = strdup(optarg);
             break;
        case 'r':
             if (optnum == 1) {
               fprintf(stderr, "Operation flag must come first.\n");
               usage(stderr, prog);
               exit(1);
             }
             if (op == DELETE) {
               fprintf(stderr, "WARNING: role ignored for deletion.\n");
             } else {
               if (G_str[STR_ROLE] != NULL) {
                 fprintf(stderr, "WARNING: role %s replaced by %s.\n",
                         G_str[STR_ROLE], optarg);
                 free(G_str[STR_ROLE]);
               }
               G_str[STR_ROLE] = strdup(optarg);
             }
             break;
        case 'd':
             if (optnum == 1) {
               fprintf(stderr, "Operation flag must come first.\n");
               usage(stderr, prog);
               exit(1);
             }
             if (G_str[STR_DB] != NULL) {
               fprintf(stderr, "WARNING: database %s replaced by %s.\n",
                         G_str[STR_DB], optarg);
                 free(G_str[STR_DB]);
             }
             G_str[STR_DB] = strdup(optarg);
             break;
        case 'x':
             if (optnum == 1) {
               fprintf(stderr, "Operation flag must come first.\n");
               usage(stderr, prog);
               exit(1);
             }
             if (sscanf(optarg, "%hd", &skip) != 1) {
               fprintf(stderr, "Invalid number of lines to skip '%s'.\n",
                               optarg);
               usage(stderr, prog);
               exit(1);
             } 
             if (skip <= 0) {
               fprintf(stderr, "Invalid number of lines to skip %hd.\n",
                               skip);
               usage(stderr, prog);
               exit(1);
             } 
             break;
        case '?':
             usage(stdout, prog);
             exit(0);
             break; /*NOTREACHED*/
        default:
             usage(stderr, prog);
             exit(1);
             break; /*NOTREACHED*/
      }
    }
    if (op == NO_OP) {
      fprintf(stderr, "Operation to perform unspecified.\n");
      usage(stderr, prog);
      exit(1);
    }
    argc -= optind;
    argv += optind;
    // Any file name provided?
    if (argc != 1) {
      usage(stderr, prog);
      exit(1);
    }
    // Check that the file exists ...
    if (stat(argv[0], &buf) == -1) {
      perror(argv[0]);
      fputc('\n', stderr);
      usage(stderr, prog);
      exit(1);
    }
    // Connect to Postgres
    if (G_str[STR_DB] == NULL) {
      G_str[STR_DB] = strdup("postgres");
    }
    connect();
    // Set the notice receiver
    (void)PQsetNoticeReceiver(G_conn, ignore_notice, NULL);
    // Read the file
    if ((fp = fopen(argv[0], "r")) != NULL) {
      while ((ch = fgetc(fp)) != EOF) {
        switch(ch) {
          case '"':
               blind++;
               blind = (blind % 2);
               break;
          case '\\': // Escape
               ch = fgetc(fp);
               if (ch == EOF) { // Ooops
                 fprintf(stderr,
                   "Warning: Escape character just before end of file in %s\n",
                         argv[0]);
                 fclose(fp);
                 exit(0);
               }
               break;
          case '\n':
               // Carriage returns aren't expected to be quoted 
               linecnt++;
               fieldnum = 1;
               blind = 0; // Just in case 
               if (linecnt > skip) {
                 account[aidx] = '\0';
                 defpass[pidx] = '\0';
                 // Create the account
                 if (op == CREATE) {
                   if (create_account(account, defpass, G_str[STR_ROLE])) {
                     printf(" -- account %s%s created\n", 
                             (isdigit(*account) ? "u" : ""),
                             account);
                     cnt++;
                   }
                 } else {
                   if (delete_account(account)) {
                     printf(" -- account %s%s deleted\n",
                             (isdigit(*account) ? "u" : ""),
                             account);
                     cnt++;
                   }
                 }
               }
               account[0] = '\0';
               aidx = 0;
               defpass[0] = '\0';
               pidx = 0;
               break;
          default:
               if (ch == G_sep) {
                 if (!blind) {
                   if (fieldnum == G_id) {
                     account[aidx] = '\0';
                   }
                   if (fieldnum == G_pwd) {
                     defpass[pidx] = '\0';
                   }
                   fieldnum++;
                   if (fieldnum == G_id) {
                     aidx = 0;
                   }
                   if (fieldnum == G_pwd) {
                     pidx = 0;
                   }
                 }
               } else {
                 if (fieldnum == G_id) {
                   account[aidx++] = ch;
                 }
                 if (fieldnum == G_pwd) {
                   defpass[pidx++] = ch;
                 }
               }
               break;
        }
      }
      fclose(fp);
      printf("*** %d account%s %s ***\n",
             cnt,
             (cnt == 1 ? "": "s"),
             (op == CREATE ? "created" : "deleted"));
    }
    return 0;
}
