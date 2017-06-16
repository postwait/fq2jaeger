#define _REENTRANT

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <fq.h>
#include <curl/curl.h>
#include <getopt.h>

static int foreground = 0;
static const int MAX_MSGS = 500;
static int num_retries = 0;
static char *fq_host = "localhost";
static unsigned short fq_port = 8765;
static char *fq_user = "fq2jaeger";
static char *fq_pass = "none";
static char *fq_exchange = "logging";
static char *fq_route = "prefix:\"zipkin.thrift.\"";
static int jaeger_timeout_ms = 5000;
static int jaeger_connect_timeout_ms = 1000;

static char *jaeger_host = "localhost";
static unsigned short jaeger_port = 14268;

static void
fq_logger(fq_client client, const char *str) {
  fprintf(stderr, "fq: %s\n", str);
}

static void
my_auth_handler(fq_client c, int error) {
  fq_bind_req *breq;
 
  if(error) return;

  printf("attempting bind\n"); 
  breq = malloc(sizeof(*breq));
  memset(breq, 0, sizeof(*breq));
  int exchange_len = strlen(fq_exchange);
  memcpy(breq->exchange.name, fq_exchange, exchange_len);
  breq->exchange.len = exchange_len;
  breq->flags = FQ_BIND_TRANS;
  breq->program = strdup(fq_route);
  fq_client_bind(c, breq);
}

static void
my_bind_handler(fq_client c, fq_bind_req *breq) {
  (void)c;
  printf("route set -> %u\n", breq->out__route_id);
  if(breq->out__route_id == FQ_BIND_ILLEGAL) {
    fprintf(stderr, "Failure to bind...\n");
    exit(-1);
  }
}

fq_hooks fqhooks = {
 .version = FQ_HOOKS_V1,
 .auth = my_auth_handler,
 .bind = my_bind_handler
};

void
jaeger_submit(fq_msg **msg, int cnt) {
  static CURL *curl;
  static unsigned char *payload;
  static size_t payload_len = 128*1024;

  int i, retries = num_retries;
  CURLcode code;
  long httpcode;
  char error[CURL_ERROR_SIZE];
  size_t plen = 5; /* 1 + 4 for list header */
  int32_t ncnt = htonl(cnt);

  for(i=0;i<cnt;i++) plen += msg[i]->payload_len;
  if(plen > payload_len) {
    free(payload);
    payload_len = plen;
  }
  if(!payload) payload = malloc(payload_len);
  payload[0] = 12; /* thrift type struct */
  memcpy(&payload[1], &ncnt, 4);
  plen = 5; /* reset to just struct list */
  for(i=0; i<cnt;i++) {
    memcpy(payload+plen, msg[i]->payload, msg[i]->payload_len);
    plen += msg[i]->payload_len;
  }

  if(!curl) {
    char url[1024];
    struct curl_slist *headers=NULL;
    snprintf(url, sizeof(url), "http://%s:%d/api/traces?format=zipkin.thrift",
             jaeger_host, jaeger_port);
    headers = curl_slist_append(headers, "Content-Type: application/x-thrift");
    curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 131072);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, jaeger_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, jaeger_connect_timeout_ms);
  }
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, (void *)payload);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, payload_len);
  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, error);

  do {
    error[0] = '\0';
    httpcode = 0;
    code = curl_easy_perform(curl);
    if(CURLE_OK == code)
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpcode);
  } while(retries > 0 && (httpcode < 200 || httpcode > 299));
}

void usage(const char *prog) {
  fprintf(stderr, "%s\n", prog);
  fprintf(stderr, "\nThis program listens for zipkin.thrift messages from Fq\n");
  fprintf(stderr, "and pushes lists of those messages to Jaeger for storage\n");
  fprintf(stderr, "and reporting.  All options below have sane defaults.\n");
  fprintf(stderr, "Both the fq and jaeger hosts default to localhost.\n");
  fprintf(stderr, "\n\t=== global options ===\n");
  fprintf(stderr, "\t-D\t\t\tprevent from daemonizing\n");
  fprintf(stderr, "\t-h\t\t\tusage message\n");
  fprintf(stderr, "\n\t=== fq options ===\n");
  fprintf(stderr, "\t-f fqhost:port\t\ttarget Fq instance\n");
  fprintf(stderr, "\t-u user\t\t\tFq user (connstr)\n");
  fprintf(stderr, "\t-p pass\t\t\tFq password\n");
  fprintf(stderr, "\t-r route\t\tFq route\n");
  fprintf(stderr, "\n\t=== jaeger options ===\n");
  fprintf(stderr, "\t-J jaegerhost:port\ttarget Jaeger isntance\n");
  fprintf(stderr, "\t-T timeout(ms)\t\tcurl timeout to jaeger\n");
  fprintf(stderr, "\t-C timeout(ms)\t\tcurl connect timeout to jaeger\n");
}

int main(int argc, char **argv) {
  int ch;
  char *colon;
  while((ch = getopt(argc, argv, "Df:J:u:p:r:T:C:")) != -1) {
    switch(ch) {
    /* self */
    case 'D': foreground = 1; break;

    /* fq */
    case 'u': fq_user = strdup(optarg); break;
    case 'p': fq_pass = strdup(optarg); break;
    case 'r': fq_route = strdup(optarg); break;
    case 'f':
      fq_host = strdup(optarg);
      if(NULL != (colon = strchr(fq_host, ':'))) {
        *colon++ = '\0';
        fq_port = atoi(colon);
      }
      break;

    /* jaeger */
    case 'R': num_retries = atoi(optarg); break;
    case 'J':
      jaeger_host = strdup(optarg);
      if(NULL != (colon = strchr(jaeger_host, ':'))) {
        *colon++ = '\0';
        jaeger_port = atoi(colon);
      }
      break;
    case 'T': jaeger_timeout_ms = atoi(optarg); break;
    case 'C': jaeger_connect_timeout_ms = atoi(optarg); break;
    case 'h': /* FALLTHRU */
    default: usage(argv[0]); exit(-1);
    }
  }

  if(!foreground) {
    int fd = open("/dev/null", O_RDWR);
    if(fd < 0) {
      fprintf(stderr, "Failed to shutdown stdin/stdout.\n");
      exit(-1);
    }
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    close(fd);
    if(fork()) exit(0);
    setsid();
    if(fork()) exit(0);
  }

  fq_client fq;
  fq_client_init(&fq, 0, fq_logger);
  fq_client_hooks(fq, &fqhooks);
  fq_client_creds(fq, fq_host, fq_port, fq_user, fq_pass);
  fq_client_heartbeat(fq, 1000);
  fq_client_set_backlog(fq, 10000, 100);
  fq_client_connect(fq);

  fq_msg *msg[MAX_MSGS];
  while(1) {
    int i, cnt = 0;
    while(cnt < MAX_MSGS && NULL != (msg[cnt] = fq_client_receive(fq))) {
      cnt++;
    }
    if(cnt > 0) {
      jaeger_submit(msg, cnt);
      for(i=0;i<cnt;i++) fq_msg_deref(msg[i]);
    } else {
      usleep(1000);
    }
  }
  return 0;
}
