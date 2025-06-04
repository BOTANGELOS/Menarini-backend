from flask import Blueprint, jsonify
import mysql.connector
import os
import re
import ast


def complete_flag(dn):
    
  conn = get_db_connection()
  cursor = conn.cursor()
  query = """SELECT `complete` FROM attachment_table WHERE `DN#` = %s;""" 
  cursor.execute(query, (dn,))
  results = cursor.fetchone()
  if results:
    results = results[0]
  cursor.close()
  conn.close()
  
  return results