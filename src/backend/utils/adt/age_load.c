/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 还差一个 数据插入结束后，对seq num 修改
 * 数据格式不对不让插入  修改
 */

#include "postgres.h"
#include "utils/json.h"

#include "utils/ag_load_edges.h"
#include "utils/ag_load_labels.h"
#include "utils/age_load.h"
#include "utils/jsonb.h"


/*
 * Helper function that recursively deallocates the contents 
 * of the passed agtype_value only. It does not deallocate
 * `value` itself.
 */
static void pfree_agtype_value_content(JsonbValue* value)
{
    int i;

    // guards against stack overflow due to deeply nested agtype_value
    check_stack_depth();

    switch (value->type)
    {
        case jbvNumeric:
            pfree(value->val.numeric);
            break;

        case jbvString:
            /*
             * The char pointer (val.string.val) is not free'd because
             * it is not allocated by an agtype helper function.
             */
            break;
        case jbvObject:
         for (i = 0; i < value->val.object.nPairs; i++)
            {
                pfree_agtype_value_content(&value->val.object.pairs[i].key);
                pfree_agtype_value_content(&value->val.object.pairs[i].value);
            }
            break;
        case jbvBinary:
        case jbvBool:
            /*
             * These are deallocated by the calling function.
             */
            break;

        default:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("unknown agtype")));
            break;
    }
}
static void pfree_agtype_value(JsonbValue* value)
{
    pfree_agtype_value_content(value);
    pfree(value);
}


static void pfree_agtype_in_state(JsonbParseState* value,JsonbValue* jb)
{
    pfree_agtype_value(jb);
    free(value);
}

static Jsonb* create_empty_agtype(void)
{


    JsonbParseState* jbstate = NULL;
    JsonbValue* jb;
    Jsonb* ret = NULL;
    pushJsonbValue(&jbstate, WJB_BEGIN_OBJECT, NULL);
    jb =  pushJsonbValue(&jbstate, WJB_END_OBJECT, NULL);
    ret = JsonbValueToJsonb(jb);
    pfree_agtype_in_state(jbstate,jb);
    return ret;
}

Jsonb* create_agtype_from_list(char **header, char **fields,enum jbvType* col_type,
                                size_t fields_len, int64 vertex_id)
{
      /*
     * transform props into jsonb
     */
    JsonbParseState* jbstate = NULL;
    JsonbValue* jb;
    Jsonb* ret = NULL;
    pushJsonbValue(&jbstate, WJB_BEGIN_OBJECT, NULL);

    for (size_t i = 0; i<fields_len; i++)
    {
        JsonbValue vjv, kjv;
        char *key, *val;

        key = header[i];
        kjv.type = jbvString;
        kjv.val.string.val = key;
        kjv.val.string.len = strlen(key);
        pushJsonbValue(&jbstate, WJB_KEY, &kjv);

   

        val = fields[i];
        vjv.type = col_type[i];
        switch (col_type[i])
        {
        case jbvString:
            /* code */
             vjv.val.string.val = val;
             vjv.val.string.len = strlen(val);
            break;
        case jbvNumeric:
            /* code */
            vjv.val.numeric = DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(val), 0, -1));
            break;
        default:
            break;
        }
       
       
        pushJsonbValue(&jbstate, WJB_VALUE, &vjv);
    }
    jb = pushJsonbValue(&jbstate, WJB_END_OBJECT, NULL);
    ret = JsonbValueToJsonb(jb);
    pfree_agtype_in_state(jbstate,jb);
    return ret;
}
Jsonb* create_jsonb_from_list_i(char **header, char **fields,enum jbvType* col_type,
                                  size_t fields_len, size_t start_index,size_t end_index)
{
    
       StringInfo result;
    result = makeStringInfo();
    const char *sep = ",";
    bool needsep = false;
    appendStringInfoChar(result, '{');
    int i;
    for (i = 0; i < fields_len; i++) {
        if(i==start_index || i ==end_index){
            continue;
        }
        if (needsep) {
            appendStringInfoString(result, sep);
        }
        needsep = true;
        escape_json(result, header[i]);
        appendStringInfoChar(result, ':');
        escape_json(result, fields[i]);
    }
    appendStringInfoChar(result, '}');
    Jsonb *jsonb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,PointerGetDatum(result->data)));
    pfree(result);
    return jsonb;
}

Jsonb* create_agtype_from_list_i(char **header, char **fields,enum jbvType* col_type,
                                  size_t fields_len, size_t start_index,size_t end_index)
{
    if (2== fields_len)
    {
        return create_empty_agtype();
    }

    JsonbParseState* jbstate = NULL;
    JsonbValue* jb;
    Jsonb* ret = NULL;
    size_t i;
   
    pushJsonbValue(&jbstate, WJB_BEGIN_OBJECT, NULL);
    for (i = 0; i<fields_len; i++)
    {
        if(i==start_index || i ==end_index){
            continue;
        }
        JsonbValue vjv, kjv;
        char *key, *val;

        key = header[i];
        kjv.type = jbvString;
        kjv.val.string.val = key;
        kjv.val.string.len = strlen(key);
        pushJsonbValue(&jbstate, WJB_KEY, &kjv);

   

        val = fields[i];
        vjv.type = col_type[i];
          switch (col_type[i])
        {
        case jbvString:
            /* code */
             vjv.val.string.val = val;
             vjv.val.string.len = strlen(val);
            break;
        case jbvNumeric:
            /* code */
            vjv.val.numeric = DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(val), 0, -1));
            break;
        default:
            break;
        }
        pushJsonbValue(&jbstate, WJB_VALUE, &vjv);
    }

    jb = pushJsonbValue(&jbstate, WJB_END_OBJECT, NULL);
    ret = JsonbValueToJsonb(jb);
    pfree_agtype_in_state(jbstate,jb);
    return ret;
    
 
}
Jsonb* create_jsonb_from_list(char **header, char **fields,enum jbvType* col_type,
                                size_t fields_len, int64 vertex_id)
{
    StringInfo result;
    result = makeStringInfo();
    const char *sep = ",";
    bool needsep = false;
    appendStringInfoChar(result, '{');
    int i;
    for (i = 0; i < fields_len; i++) {
        if (needsep) {
            appendStringInfoString(result, sep);
        }
        needsep = true;
        escape_json(result, header[i]);
        appendStringInfoChar(result, ':');
        escape_json(result, fields[i]);
    }
    appendStringInfoChar(result, '}');
    Jsonb *jsonb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,PointerGetDatum(result->data)));
    pfree(result);
    return jsonb;
}

void insert_edge_simple(Oid graph_id, char* label_name, Graphid edge_id,
                        Graphid start_id, Graphid end_id,
                        Jsonb* edge_properties)
{

    Datum values[6];
    bool nulls[4] = {false, false, false, false};
   
    HeapTuple tuple;
    Relation label_relation;


    values[0] = GraphidGetDatum(edge_id);
    values[1] = GraphidGetDatum(start_id);
    values[2] = GraphidGetDatum(end_id);
    // Jsonb* out = JsonbValueToJsonb(edge_properties);
    values[3] = JsonbPGetDatum(edge_properties);
  label_relation = table_open(get_labid_relid (graph_id, get_labname_labid(label_name,
                                                  graph_id)),
                                RowExclusiveLock);

    tuple = heap_form_tuple(RelationGetDescr(label_relation),
                            values, nulls);
    heap_insert(label_relation, tuple,
                GetCurrentCommandId(true), 0, NULL);
    table_close(label_relation, RowExclusiveLock);
    CommandCounterIncrement();

}

void  insert_vertex_simple(Oid graph_id, char* label_name,
                          Graphid vertex_id,
                          Jsonb* vertex_properties)
{

    Datum values[2];
    bool nulls[2] = {false, false};
 
    HeapTuple tuple;
    Relation label_relation;

    values[0] = GraphidGetDatum(vertex_id);
    values[1] = JsonbPGetDatum(vertex_properties);

                                       // 这里打开表
    label_relation = table_open(get_labid_relid (graph_id, get_labname_labid(label_name,
                                                  graph_id)),
                                RowExclusiveLock);
    tuple = heap_form_tuple(RelationGetDescr(label_relation),
                            values, nulls);
    heap_insert(label_relation, tuple,
                GetCurrentCommandId(true), 0, NULL);
    table_close(label_relation, RowExclusiveLock);
    CommandCounterIncrement();
}



Datum load_labels_from_file(PG_FUNCTION_ARGS)
{

    Name graph_name;
    Name label_name;
    text* file_path;
    char* graph_name_str;
    char* label_name_str;
    char* file_path_str;
    Oid graph_id;
    int32 label_id;
    bool id_field_exists;

 

    if (PG_ARGISNULL(0))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("graph name must not be NULL")));
    }

    if (PG_ARGISNULL(1))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("label name must not be NULL")));
    }

    if (PG_ARGISNULL(2))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("file path must not be NULL")));
    }

    graph_name = PG_GETARG_NAME(0);
    label_name = PG_GETARG_NAME(1);
    file_path = PG_GETARG_TEXT_P(2);
    id_field_exists = true; // PG_GETARG_BOOL(3);


    graph_name_str = NameStr(*graph_name);
    label_name_str = NameStr(*label_name);
    file_path_str = text_to_cstring(file_path);

    graph_id = get_graphname_oid(graph_name_str);
    label_id = get_labname_labid(label_name_str, graph_id);

    create_labels_from_csv_file(file_path_str, graph_name_str,
                                graph_id, label_name_str,
                                label_id, id_field_exists);
    PG_RETURN_VOID();

}

Datum load_edges_from_file(PG_FUNCTION_ARGS)
{

    Name graph_name;
    Name label_name;
    text* file_path;
    char* graph_name_str;
    char* label_name_str;
    char* file_path_str;
    Oid graph_id;
    int32 label_id;
 
    if (PG_ARGISNULL(0))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("graph name must not be NULL")));
    }

    if (PG_ARGISNULL(1))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("label name must not be NULL")));
    }

    if (PG_ARGISNULL(2))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("file path must not be NULL")));
    }

    graph_name = PG_GETARG_NAME(0);
    label_name = PG_GETARG_NAME(1);
    file_path = PG_GETARG_TEXT_P(2);

    graph_name_str = NameStr(*graph_name);
    label_name_str = NameStr(*label_name);
    file_path_str = text_to_cstring(file_path);

    graph_id = get_graphname_oid(graph_name_str);
    label_id = get_labname_labid(label_name_str, graph_id);

    create_edges_from_csv_file(file_path_str, graph_name_str,
                               graph_id, label_name_str, label_id);
 
    PG_RETURN_VOID();

}

