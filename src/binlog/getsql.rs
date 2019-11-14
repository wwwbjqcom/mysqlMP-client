/*
@author: xiao cai niao
@datetime: 2019/10/11
*/

use std::collections::HashMap;
use crate::binlog::readevent::{BinlogEvent,TableMap};
use crate::binlog::parsevalue::RowValue;
use crate::binlog::parsevalue::MySQLValue;
use failure::_core::str::from_utf8;
use hex;
use bigdecimal::ToPrimitive;
use std::error::Error;

pub fn get_command(
    row_values: &RowValue,code: &BinlogEvent,
    table_cols_info: &mut HashMap<String, Vec<HashMap<String, String>>>,
    db_tbl: &String, map: &TableMap) -> Result<Vec<String>, Box<dyn Error>> {

    let mut sqls: Vec<String> = vec![];
    match table_cols_info.get(db_tbl) {
        Some(t) => {
            let cols = t;
            //let pri_idex = 0 as usize;          //主键索引
            let mut pri = &String::from("");    //主键名称
            let mut pri_info: HashMap<String, usize> = HashMap::new();
            for (idx,r) in cols.iter().enumerate(){
                if r.get("COLUMN_KEY").unwrap() == &String::from("PRI") {
                    pri = r.get("COLUMN_NAME").unwrap();
                    pri_info.insert(pri.parse().unwrap(), idx);
                }
            }

            match code {
                BinlogEvent::UpdateEvent => {
                    //println!("-- Update Row Value");
                    let mut a = vec![];
                    let mut rows = vec![];
                    for (i, row) in row_values.rows.iter().enumerate() {
                        match i % 2 {
                            0 => {
                                a.push(row);
                            }
                            1 => {
                                a.push(row);
                                rows.push(a);
                                a = vec![];
                            }
                            _ => {}
                        }
                    }
                    for row in rows{
                        let befor_value = row[0];
                        let after_value = row[1];
                        let v = out_update(befor_value, after_value, cols, &pri_info,map);
                        sqls.push(v);
                    }
                }
                BinlogEvent::WriteEvent => {
                    //println!("-- Insert Row Value");
                    for row in &row_values.rows {
                        let v = out_insert(row, cols, map);
                        sqls.push(v);
                    }
                }
                BinlogEvent::DeleteEvent => {
                    //println!("-- Delete Row Value");
                    for row in &row_values.rows {
                        let v = out_delete(row, cols, &pri_info, map);
                        sqls.push(v);
                    }
                }
                _ => {}
            }
        }
        None => {
            let e: Box<dyn Error> = format!("no columns info: {},{:?}", db_tbl, table_cols_info).into();
            return Err(e);
        }
    }
    return Ok(sqls);
}


enum GetType{
    GetWhere,
    GetSet
}


pub fn out_delete(
    row_value: &Vec<Option<MySQLValue>>,
    table_cols_info: &Vec<HashMap<String, String>>,
    pri_info: &HashMap<String, usize>,
    map: &TableMap) -> String {

    let mut sql = format!("DELETE FROM {}.{} ", map.database_name, map.table_name);
    let where_str = get_where_str(row_value, table_cols_info, pri_info);
    sql.push_str(&where_str);
    sql
}

pub fn out_insert(
    row_value: &Vec<Option<MySQLValue>>,
    table_cols_info: &Vec<HashMap<String, String>>,
    map: &TableMap) -> String {

    let mut sql = format!("INSERT INTO {}.{}", map.database_name,map.table_name);
    let col_str = get_insert_col_str(table_cols_info);
    let value_str = get_values_str(row_value, table_cols_info);
    sql.push_str(&col_str);
    sql.push_str(" ");
    sql.push_str(&value_str);
    sql
}

fn get_values_str(values: &Vec<Option<MySQLValue>>, table_cols_info: &Vec<HashMap<String, String>>) -> String{
    let mut values_str = format!("VALUES(");
    let value_len = values.len();
    for (idx,value) in values.iter().enumerate() {
        let col_type = table_cols_info[idx].get("COLUMN_TYPE").unwrap();
        values_str.push_str(get_values_info(value, col_type).as_ref());
        if idx < value_len - 1{
            values_str.push_str(",");
        }else {
            values_str.push_str(");");
        }
    }
    values_str
}

fn get_values_info(value: &Option<MySQLValue>, col_type: &String) -> String {
    let mut value_str = String::from("");
    match value {
        Some(MySQLValue::String(t)) => {
            value_str.push_str(&format!("'{}'",t));
        }
        Some(MySQLValue::Blob(t)) => {
            match col_type.find("text") {
                Some(_) => {
                    value_str.push_str(&format!("'{}'",from_utf8(t).unwrap()));
                    return value_str;
                }
                None => {}
            }
            match col_type.find("char") {
                Some(_) => {
                    value_str.push_str(&format!("'{}'",from_utf8(t).unwrap()));
                    return value_str;
                }
                None => {}
            }

            if t.len()> 0{
                value_str.push_str(&format!("0x{}",hex::encode(t)));
            }else {
                value_str.push_str(&format!("Null"));
            }
        }
        Some(MySQLValue::Timestamp {unix_time, subsecond}) => {
            value_str.push_str(&format!("{}.{}", unix_time, subsecond));
        }
        Some(MySQLValue::Enum(t)) => {
            value_str.push_str(&format!("{}",t));
        }
        Some(MySQLValue::DateTime {year, month, day, hour, minute, second, subsecond}) => {
            value_str.push_str(&format!("'{}-{}-{} {}:{}:{}.{}'", year,month,day,hour,minute,second,subsecond));
        }
        Some(MySQLValue::Double(t)) => {
            value_str.push_str(&format!("{}", t));
        }
        Some(MySQLValue::Float(t)) => {
            value_str.push_str(&format!("{}",t));
        }
        Some(MySQLValue::Year(t)) => {
            value_str.push_str(&format!("{}",t));
        }
        Some(MySQLValue::Decimal(t)) => {
            value_str.push_str(&format!("{}",t.to_f64().unwrap()));
        }
        Some(MySQLValue::SignedInteger(t)) => {
            value_str.push_str(&format!("{}", t));
        }
        Some(MySQLValue::Json(t)) => {
            value_str.push_str(&format!("'{}'", serde_json::to_string(&t).unwrap()));
        }
        Some(MySQLValue::Null) => {
            value_str.push_str(&format!("Null"));
        }
        Some(MySQLValue::Time {hours, minutes, seconds, subseconds}) => {
            value_str.push_str(&format!("'{}:{}:{}.{}'",hours, minutes, seconds, subseconds));
        }
        Some(MySQLValue::Date {year, month, day}) => {
            value_str.push_str(&format!("'{}-{}-{}'", year, month, day));
        }
        _ => {
            println!("{:?}",value);
        }
    }
    value_str
}

fn get_insert_col_str(table_cols_info: &Vec<HashMap<String, String>>) -> String {
    let mut col_str = format!("(");
    let cols = table_cols_info.len();
    for (idx,col_info) in table_cols_info.iter().enumerate() {
        if idx < cols -1 {
            col_str.push_str(&format!("{},",col_info.get("COLUMN_NAME").unwrap()));
        }else {
            col_str.push_str(&format!("{})",col_info.get("COLUMN_NAME").unwrap()));
        }
    }
    col_str
}


pub fn out_update(
    befor_value: &Vec<Option<MySQLValue>>,
    after_value: &Vec<Option<MySQLValue>>,
    table_cols_info: &Vec<HashMap<String, String>>,
    pri_info: &HashMap<String, usize>,
    map: &TableMap) -> String {

    let mut sql = format!("UPDATE {}.{} SET ", map.database_name,map.table_name);
    let where_str = get_where_str(befor_value, table_cols_info, pri_info);
    //String::from("{:?}",befor_value)
    let set_str = get_set_str(after_value, table_cols_info, map);
    sql.push_str(&set_str);
    sql.push_str(&where_str);
    sql
}

fn get_set_str(value: &Vec<Option<MySQLValue>>,table_cols_info: &Vec<HashMap<String, String>>, _map: &TableMap) -> String {
    let mut set_str = "".to_string();
    let value_len = value.iter().len();
    for (idx, v) in value.iter().enumerate() {
        let col = table_cols_info[idx].get("COLUMN_NAME").unwrap();
        let col_type = table_cols_info[idx].get("COLUMN_TYPE").unwrap();
        set_str.push_str(&get_value_str(v, col, &col_type, GetType::GetSet));
        if idx < value_len - 1 {
            set_str.push_str(", ");
        }
    }
    set_str
}


fn get_where_str(value: &Vec<Option<MySQLValue>>,table_cols_info: &Vec<HashMap<String, String>>, pri_info: &HashMap<String, usize>) -> String {
    let mut where_str = " WHERE ".to_string();
    let cols = pri_info.len();
    if pri_info.len() > 0 {
        let mut tmp = 1;
        for (col, idx) in pri_info{
            let value = &value[*idx];
            let col_type = table_cols_info[*idx].get("COLUMN_TYPE").unwrap();
            where_str.push_str(&get_value_str(value, col, col_type,GetType::GetWhere));
            if tmp < cols{
                where_str.push_str(" AND ");
            }
            else {
                where_str.push_str(";");
            }
            tmp += 1;
        }
    }

//    if pri.len() > 0 {
//        let value = &value[pri_idex];
//        let col_type = table_cols_info[pri_idex].get("COLUMN_TYPE").unwrap();
//        where_str.push_str(&get_value_str(value, pri, col_type,GetType::GetWhere));
//        where_str.push_str(";");
//    }
    else {
        let value_len = value.iter().len();
        for (idx, v) in value.iter().enumerate(){
            let col = table_cols_info[idx].get("COLUMN_NAME").unwrap();
            let col_type = table_cols_info[idx].get("COLUMN_TYPE").unwrap();
            where_str.push_str(get_value_str(v, col, &col_type, GetType::GetWhere).as_ref());
            if idx < value_len - 1 {
                where_str.push_str(" AND ");
            } else {
                where_str.push_str(";");
            }
        }
    }

    where_str
}

fn get_value_str(value: &Option<MySQLValue>,col: &String, col_type: &String, get_type: GetType) -> String {
    let mut where_str = String::from("");
    match value {
        Some(MySQLValue::String(t)) => {
            where_str.push_str(&format!("{}='{}'",col, t));
        }
        Some(MySQLValue::Blob(t)) => {
            let col_type = col_type;
            match col_type.find("text") {
                Some(_) => {
                    where_str.push_str(&format!("{}='{}'",col, from_utf8(t).unwrap()));
                    return where_str;
                }
                None => {}
            }
            match col_type.find("char") {
                Some(_) => {
                    where_str.push_str(&format!("{}='{}'",col, from_utf8(t).unwrap()));
                    return where_str;
                }
                None => {}
            }

            if t.len()> 0{
                where_str.push_str(&format!("{}=0x{}",col,hex::encode(t)));
            }else {
                match get_type {
                    GetType::GetWhere => where_str.push_str(&format!("{} is Null",col)),
                    GetType::GetSet => where_str.push_str(&format!("{}=Null",col)),
                }
            }
        }
        Some(MySQLValue::Timestamp {unix_time, subsecond}) => {
            where_str.push_str(&format!("{}={}.{}", col, unix_time, subsecond));
        }
        Some(MySQLValue::Enum(t)) => {
            where_str.push_str(&format!("{}={}",col,t));
        }
        Some(MySQLValue::DateTime {year, month, day, hour, minute, second, subsecond}) => {
            where_str.push_str(&format!("{}='{}-{}-{} {}:{}:{}.{}'", col,year,month,day,hour,minute,second,subsecond));
        }
        Some(MySQLValue::Double(t)) => {
            where_str.push_str(&format!("{}={}",col, t));
        }
        Some(MySQLValue::Float(t)) => {
            where_str.push_str(&format!("{}={}",col, t));
        }
        Some(MySQLValue::Year(t)) => {
            where_str.push_str(&format!("{}={}",col, t));
        }
        Some(MySQLValue::Decimal(t)) => {
            where_str.push_str(&format!("{}={}",col, t.to_f64().unwrap()));
        }
        Some(MySQLValue::SignedInteger(t)) => {
            where_str.push_str(&format!("{}={}",col, t));
        }
        Some(MySQLValue::Json(t)) => {
            where_str.push_str(&format!("{}='{}'",col, serde_json::to_string(&t).unwrap()));
        }
        Some(MySQLValue::Null) => {
            match get_type {
                GetType::GetWhere => where_str.push_str(&format!("{} is Null",col)),
                GetType::GetSet => where_str.push_str(&format!("{}=Null",col)),
            }
        }
        Some(MySQLValue::Time {hours, minutes, seconds, subseconds}) => {
            where_str.push_str(&format!("{}='{}:{}:{}.{}'",col, hours, minutes, seconds, subseconds));
        }
        Some(MySQLValue::Date {year, month, day}) => {
            where_str.push_str(&format!("{}='{}-{}-{}'",col, year, month, day));
        }
        _ => {
            println!("{:?}",value);
        }
    }
    where_str
}