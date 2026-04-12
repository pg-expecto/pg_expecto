-- Copyright 2026 Ринат (pg_expecto)
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
-- http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
  
-- before_start.sql
-- version 6.0
--Вариант по умолчанию - НИЧЕГО НЕ МЕНЯТЬ
SELECT CURRENT_TIMESTAMP ;
--Вариант по умолчанию - НИЧЕГО НЕ МЕНЯТЬ

--------------------------------------------------------
-- Изменения/добавления тестовых таблиц
ALTER TABLE pgbench_history DROP CONSTRAINT IF EXISTS pgbench_history_aid_fkey ; 
ALTER TABLE pgbench_history DROP CONSTRAINT IF EXISTS pgbench_history_bid_fkey ; 
ALTER TABLE pgbench_history DROP CONSTRAINT IF EXISTS pgbench_history_tid_fkey ; 

-- 1. Добавить столбец для предотвращения HOT-обновлений
ALTER TABLE pgbench_history ADD COLUMN IF NOT EXISTS random_fill INTEGER;

-- 2. Создать индекс на этот столбец (обязательно для запрета HOT)
CREATE INDEX IF NOT EXISTS idx_pgbench_history_random_fill ON pgbench_history(random_fill);

-- Изменения/добавления тестовых таблиц
--------------------------------------------------------
