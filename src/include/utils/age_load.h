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
 */

#include "commands/sequence.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/graph.h"
#include "utils/jsonb.h"



#ifndef INCUBATOR_AGE_ENTITY_CREATOR_H
#define INCUBATOR_AGE_ENTITY_CREATOR_H

Jsonb* create_agtype_from_list(char **header, char **fields,enum jbvType* col_type,
                                size_t fields_len, int64 vertex_id);
Jsonb* create_jsonb_from_list(char **header, char **fields,enum jbvType* col_type,
                                size_t fields_len, int64 vertex_id);
Jsonb* create_jsonb_from_list_i(char **header, char **fields,enum jbvType* col_type,
                                  size_t fields_len, size_t start_index, size_t end_index);
Jsonb* create_agtype_from_list_i(char **header, char **fields,enum jbvType* col_type,
                                  size_t fields_len, size_t start_index, size_t end_index);
void insert_vertex_simple(Oid graph_id, char* label_name, Graphid vertex_id,
                          Jsonb* vertex_properties);
void insert_edge_simple(Oid graph_id, char* label_name, Graphid edge_id,
                        Graphid start_id, Graphid end_id,
                        Jsonb* end_properties);

#endif //INCUBATOR_AGE_ENTITY_CREATOR_H
