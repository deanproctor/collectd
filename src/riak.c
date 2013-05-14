/**
 * collectd - src/riak.c, based on src/nginx.c
 * Copyright (C) 2013  Dean Proctor
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author:
 *   Dean Proctor <dproctor at basho.com>
 **/

#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include "configfile.h"

#include <curl/curl.h>
#include <pthread.h>
#include <ei.h>

static char *stats_url   = NULL;
static char *repl_url    = NULL;
static char *check_repl  = NULL;

static CURL *curl = NULL;

static char   riak_buffer[16384];
static size_t riak_buffer_len = 0;
static char   riak_curl_error[CURL_ERROR_SIZE];

static const char *config_keys[] =
{
  "StatsURL",
  "ReplURL",
  "CheckRepl"
};
static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);

static const char *stats_metrics[] = {"node_gets", "node_gets_total", "node_puts", "node_puts_total",
    "vnode_gets", "vnode_gets_total", "vnode_puts", "vnode_puts_total", "read_repairs", 
    "read_repairs_total", "coord_redirs_total", "node_get_fsm_time_mean", "node_get_fsm_time_median",
    "node_get_fsm_time_95", "node_get_fsm_time_100", "node_put_fsm_time_mean",
    "node_put_fsm_time_median", "node_put_fsm_time_95", "node_put_fsm_time_100", 
    "node_get_fsm_objsize_mean", "node_get_fsm_objsize_median", "node_get_fsm_objsize_95",
    "node_get_fsm_objsize_100", "node_get_fsm_siblings_mean", "node_get_fsm_siblings_median",
    "node_get_fsm_siblings_95", "node_get_fsm_siblings_100", "memory_processes_used",
    "sys_process_count", "pbc_connects", "pbc_active"}; 

static int stats_metrics_num = STATIC_ARRAY_SIZE (stats_metrics);


static const char *repl_metrics[] = {"queue_length", "queue_byte_size", "queue_percentage", 
    "dropped_count", "local_leader_message_queue_len", "local_leader_heap_size"};

static int repl_metrics_num = STATIC_ARRAY_SIZE (repl_metrics);

static size_t riak_curl_callback (void *buf, size_t size, size_t nmemb,
    void __attribute__((unused)) *stream)
{
  size_t len = size * nmemb;

  /* Check if the data fits into the memory. If not, truncate it. */
  if ((riak_buffer_len + len) >= sizeof (riak_buffer))
  {
    assert (sizeof (riak_buffer) > riak_buffer_len);
    len = (sizeof (riak_buffer) - 1) - riak_buffer_len;
  }

  if (len <= 0)
    return (len);

  memcpy (&riak_buffer[riak_buffer_len], buf, len);
  riak_buffer_len += len;
  riak_buffer[riak_buffer_len] = 0;

  return (len);
} /* size_t riak_curl_callback */

static int config_set_string (char **var, const char *value)
{
  if (*var != NULL)
  {
    free (*var);
    *var = NULL;
  }

  if ((*var = strdup (value)) == NULL)
    return (1);
  else
    return (0);
} /* int config_set_string */

static int config (const char *key, const char *value)
{
  if (strcasecmp (key, "statsurl") == 0)
    return (config_set_string (&stats_url, value));
  else if (strcasecmp (key, "replurl") == 0)
    return (config_set_string (&repl_url, value));
  else if (strcasecmp (key, "checkrepl") == 0)
    return (config_set_string (&check_repl, value));
  else
    return (-1);
} /* int config */

static int init_curl (char *url)
{
  struct curl_slist *headerlist = NULL;

  if (curl != NULL)
    curl_easy_cleanup (curl);

  if ((curl = curl_easy_init ()) == NULL)
  {
    ERROR ("riak plugin: curl_easy_init failed.");
    return (-1);
  }

  headerlist = curl_slist_append(headerlist, "Accept: text/plain");

  curl_easy_setopt (curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt (curl, CURLOPT_WRITEFUNCTION, riak_curl_callback);
  curl_easy_setopt (curl, CURLOPT_USERAGENT, PACKAGE_NAME"/"PACKAGE_VERSION);
  curl_easy_setopt (curl, CURLOPT_ERRORBUFFER, riak_curl_error);
  curl_easy_setopt (curl, CURLOPT_HTTPHEADER, headerlist);

  if (url != NULL)
  {
    curl_easy_setopt (curl, CURLOPT_URL, url);
  }

  curl_easy_setopt (curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt (curl, CURLOPT_MAXREDIRS, 50L);

  return (0);
} /* void init */

static void submit (char *type, char *inst, long long value)
{
  value_t values[1];
  value_list_t vl = VALUE_LIST_INIT;

  values[0].gauge = value;

  vl.values = values;
  vl.values_len = 1;
  sstrncpy (vl.host, hostname_g, sizeof (vl.host));
  sstrncpy (vl.plugin, "riak", sizeof (vl.plugin));
  sstrncpy (vl.plugin_instance, "", sizeof (vl.plugin_instance));
  sstrncpy (vl.type, type, sizeof (vl.type));

  if (inst != NULL)
    sstrncpy (vl.type_instance, inst, sizeof (vl.type_instance));

  plugin_dispatch_values (&vl);
} /* void submit */

static int read_stats (char *url, char *type)
{
  int i, j;

  char *ptr;
  char *lines[160];
  int   lines_num = 0;
  char *saveptr;

  char *fields[2];
  int   fields_num;

  int  *metrics_num;
  const char **metrics = NULL;

  if (strcmp(type, "riak_stats") == 0)
  {
    metrics = stats_metrics;
    metrics_num = &stats_metrics_num;
  } 
  else if (strcmp(type, "riak_repl") == 0)
  {
    metrics = repl_metrics;
    metrics_num = &repl_metrics_num;
  }
  else
  {
    return (-1);
  }

  init_curl(url);

  if (curl == NULL)
    return (-1);
  if (url == NULL)
    return (-1);

  riak_buffer_len = 0;
  if (curl_easy_perform (curl) != CURLE_OK)
  {
    WARNING ("riak plugin: curl_easy_perform failed: %s", riak_curl_error);
    return (-1);
  }

  ptr = riak_buffer;
  saveptr = NULL;
  while ((lines[lines_num] = strtok_r (ptr, "\n\r", &saveptr)) != NULL)
  {
    ptr = NULL;
    lines_num++;
  }

  /*
   * "node_gets": 0,
   * "node_gets_total": 0,
   * "node_get_fsm_siblings_mean": 0,
   * "node_get_fsm_siblings_median": 0,
   * "node_get_fsm_siblings_95": 0,
   */
  for (i = 0; i < lines_num; i++)
  {
    fields_num = strsplit (lines[i], fields,
	(sizeof (fields) / sizeof (fields[0])));

    if (fields_num == 2)
    {
      /* Remove quotes and colon from metric name */
      fields[0]++;
      fields[0][strlen(fields[0])-2] = 0;

      /* Remove trailing comma from metric value */
      fields[1][strlen(fields[1])-1] = 0;
  
      for (j = 0; j < *metrics_num; j++) 
      {
        if (strcmp(fields[0], metrics[j]) == 0)
        {
          submit (type, fields[0], atoll(fields[1]));
          break;
        }
      }
    }
  }

  riak_buffer_len = 0;

  return (0);
} /* int read_stats */

int riak_rpc (char *node, char *cookie, char *mod, char *fun, char *arg, int index, char *match_string)
{
  int arity, fd, type, size, i;
  int match = -1;
  char atom[MAXATOMLEN];
  ei_cnode ec;

  if (ei_connect_init(&ec, "collectd", cookie, 2) < 0)
  {
    ERROR ("riak plugin: failed to initiate Erlang connection");
    return -1;
  }

  if ((fd = ei_connect(&ec, node)) < 0)
  {
    ERROR ("riak plugin: failed to connect to Riak node");
    return -1;
  }

  ei_x_buff args, reply;
  ei_x_new(&args);
  ei_x_new(&reply);

  if (arg[0] != 0)
  {
    ei_x_encode_list_header(&args, 1);
    ei_x_encode_atom(&args, arg);
  }

  ei_x_encode_empty_list(&args);

  if (ei_rpc(&ec, fd, mod, fun, args.buff, args.index, &reply) < 0)
  {
    ERROR ("riak plugin: Erlang RPC call failed to %s", node);
    return -1;
  }

  reply.index = 0;

  ei_get_type(reply.buff, &reply.index, &type, &size);

  if (type == ERL_LIST_EXT || type == ERL_NIL_EXT)
  {
    ei_decode_list_header(reply.buff, &reply.index, &arity);
  }
  else
  {
    ei_decode_tuple_header(reply.buff, &reply.index, &arity);
  }

  if (index == 0)
  {
    index = arity;
  }

  for (i = 0; i < index; i++)
  {
    atom[0] = 0;
    match = -1;
    ei_decode_atom(reply.buff, &reply.index, atom);

    if (strcmp(atom, match_string) == 0)
    {
      match = 1;
      if (index == 0)
      {
        break;
      }
    }
  }

  submit ("riak_rpc", fun, match);

  close(fd);
  ei_x_free(&args);
  ei_x_free(&reply);

  return (0);
} /* int riak_rpc */

static int riak_read (void)
{
  read_stats(stats_url, "riak_stats");
  read_stats(repl_url, "riak_repl");

  char *node = "riak@127.0.0.1";
  char *cookie = "riak";

  riak_rpc(node, cookie, "riak_core_status", "ring_status", "",  3, "");
  riak_rpc(node, cookie, "riak_core_status", "ringready", "", 1, "ok");
  riak_rpc(node, cookie, "riak_core_node_watcher", "services", "", 1, "riak_kv");
  riak_rpc(node, cookie, "net_adm", "ping", node, 1, "pong");

  return (0);
}

void module_register (void)
{
  plugin_register_config ("riak", config, config_keys, config_keys_num);
  plugin_register_read ("riak", riak_read);
} /* void module_register */

/*
 * vim: set shiftwidth=2 softtabstop=2 tabstop=8 :
 */
