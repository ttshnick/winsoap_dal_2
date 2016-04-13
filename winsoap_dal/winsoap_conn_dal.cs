using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace winsoap_dal
{
    
    public class winsoap_conn_dal
    {
        public List<string> errors = new List<string>();
        DateTime dt_const = new DateTime(2014, 1, 1, 0, 0, 0);

        private SqlConnection sqlCN = null;

        public void OpenConnection(string connectionString)
        {
            sqlCN = new SqlConnection();
            sqlCN.ConnectionString = connectionString;
            sqlCN.Open();
        }

        public void CloseConnection()
        {
            sqlCN.Close();
        }
                                                                                            //for winsoap_client  BEGIN
        public List<string[]> select_countries(int company_id)
        {
            List<string[]> select_result = new List<string[]>();
            string sql_str = string.Format(@"SELECT id, country_iso3, country_name FROM [alkhalidiah].[dbo].[countries]
                                            JOIN (SELECT 
                                                         [alkhalidiah].[dbo].[links].link_country_id
                                                  FROM 
                                                         [alkhalidiah].[dbo].[links]
                                                  WHERE
                                                         [alkhalidiah].[dbo].[links].link_company_id = {0}
                                                  GROUP BY 
                                                         [alkhalidiah].[dbo].[links].link_country_id
                                                  )
                                                  AS tmp ON
                                                   tmp.link_country_id = [alkhalidiah].[dbo].[countries].id", company_id);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(new string[] { dr["id"].ToString(), dr["country_iso3"].ToString(), dr["country_name"].ToString()});
                }
                dr.Close();
            }
            return select_result;
        }

        public List<string[]> select_cities(int company_id, string country_id)
        {
            List<string[]> select_result = new List<string[]>();
            string sql_str = string.Format(@"SELECT id, city_code, city_name FROM [alkhalidiah].[dbo].[cities]
                                            JOIN (SELECT 
                                                         [alkhalidiah].[dbo].[links].link_city_id
                                                  FROM 
                                                         [alkhalidiah].[dbo].[links]
                                                  WHERE
                                                         [alkhalidiah].[dbo].[links].link_company_id = {0}
                                                    AND
                                                         [alkhalidiah].[dbo].[links].link_country_id = {1}
                                                  GROUP BY 
                                                         [alkhalidiah].[dbo].[links].link_city_id
                                                  )
                                                  AS tmp ON
                                                   tmp.link_city_id = [alkhalidiah].[dbo].[cities].id", company_id, country_id);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(new string[] { dr["id"].ToString(), dr["city_code"].ToString(), dr["city_name"].ToString() });
                }
                dr.Close();
            }
            return select_result;
        }

        public List<string[]> select_hotels(int company_id, string country_id, string city_id) 
        {
            List<string[]> select_result = new List<string[]>();
            string sql_str = string.Format(@"SELECT id, hotel_code, hotel_name, hotel_category_name FROM [alkhalidiah].[dbo].[hotels]
                                            JOIN (SELECT 
                                                         [alkhalidiah].[dbo].[links].link_hotel_id
                                                  FROM 
                                                         [alkhalidiah].[dbo].[links]
                                                  WHERE
                                                         [alkhalidiah].[dbo].[links].link_company_id = {0}
                                                    AND
                                                         [alkhalidiah].[dbo].[links].link_country_id = {1}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_city_id = {2}
                                                  GROUP BY 
                                                         [alkhalidiah].[dbo].[links].link_hotel_id
                                                  )
                                                  AS tmp ON
                                                   tmp.link_hotel_id = [alkhalidiah].[dbo].[hotels].id
                                                    ORDER BY hotel_name", company_id, country_id, city_id);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(new string[] { dr["id"].ToString(), dr["hotel_code"].ToString(), dr["hotel_name"].ToString(), dr["hotel_category_name"].ToString() });
                }
                dr.Close();
            }
            return select_result;
        }

        public List<string[]> select_specialoffers(int company_id, string country_id, string city_id, string hotel_id)
        {
            List<string[]> select_result = new List<string[]>();
            string sql_str = string.Format(@"SELECT id, specialoffer_code FROM [alkhalidiah].[dbo].[specialoffers]
                                            JOIN (SELECT 
                                                         [alkhalidiah].[dbo].[links].link_specialoffer_id
                                                  FROM 
                                                         [alkhalidiah].[dbo].[links]
                                                  WHERE
                                                         [alkhalidiah].[dbo].[links].link_company_id = {0}
                                                    AND
                                                         [alkhalidiah].[dbo].[links].link_country_id = {1}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_city_id = {2}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_hotel_id = {3}
                                                  GROUP BY 
                                                         [alkhalidiah].[dbo].[links].link_specialoffer_id
                                                  )
                                                  AS tmp ON
                                                   tmp.link_specialoffer_id = [alkhalidiah].[dbo].[specialoffers].id", company_id, country_id, city_id, hotel_id);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(new string[] { dr["id"].ToString(), dr["specialoffer_code"].ToString()});
                }
                dr.Close();
            }
            return select_result;
        }

        public List<string> select_created(int company_id, string country_id, string city_id,
                                                string hotel_id, string specialoffer_id)
        {
            List<string> select_result = new List<string>();
            string sql_str = string.Format(@"SELECT DISTINCT price_created FROM [alkhalidiah].[dbo].[prices]
                                            WHERE prices.id IN (SELECT 
                                                         [alkhalidiah].[dbo].[links].link_price_id
                                                  FROM 
                                                         [alkhalidiah].[dbo].[links]
                                                  WHERE
                                                         [alkhalidiah].[dbo].[links].link_company_id = {0}
                                                    AND
                                                         [alkhalidiah].[dbo].[links].link_country_id = {1}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_city_id = {2}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_hotel_id = {3}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_specialoffer_id = {4}
                                                  )", 
                                                    company_id, country_id, city_id, hotel_id, specialoffer_id);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(dr["price_created"].ToString().Replace(" 00:00:00", ""));
                }
                dr.Close();
            }
            return select_result;
        }

        public List<string> select_periods_start(int company_id, string country_id, string city_id,
                                                string hotel_id, string specialoffer_id)
        {
            List<string> select_result = new List<string>();
            string sql_str = string.Format(@"SELECT DISTINCT period_begin FROM [alkhalidiah].[dbo].[periods]
                                            JOIN (SELECT 
                                                         [alkhalidiah].[dbo].[links].link_period_id
                                                  FROM 
                                                         [alkhalidiah].[dbo].[links]
                                                  WHERE
                                                         [alkhalidiah].[dbo].[links].link_company_id = {0}
                                                    AND
                                                         [alkhalidiah].[dbo].[links].link_country_id = {1}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_city_id = {2}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_hotel_id = {3}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_specialoffer_id = {4}
                                                  GROUP BY 
                                                         [alkhalidiah].[dbo].[links].link_period_id
                                                  )
                                                  AS tmp ON
                                                   tmp.link_period_id = [alkhalidiah].[dbo].[periods].id
                                                    WHERE 
                                                        [alkhalidiah].[dbo].[periods].period_end >= GETDATE()
                                                     OR
                                                        ([alkhalidiah].[dbo].[periods].period_end IS NULL)", company_id,
                                                                                                           country_id, city_id,
                                                                                                           hotel_id, specialoffer_id);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(dr["period_begin"].ToString().Replace(" 00:00:00", ""));
                }
                dr.Close();
            }
            return select_result;
        }

        public List<string> select_periods_end(int company_id, string country_id, string city_id,
                                                string hotel_id, string specialoffer_id, string period_start = null)
        {
            List<string> select_result = new List<string>();
            string sql_str_1;
            if (period_start.Equals("0"))
            {
                sql_str_1 = string.Format(@"([alkhalidiah].[dbo].[periods].period_end >= GETDATE())
                                               OR
                                             ([alkhalidiah].[dbo].[periods].period_end IS NULL)");
            }
            else
            {
                sql_str_1 = string.Format(@"[alkhalidiah].[dbo].[periods].period_begin {0}
                                                    AND 
                                                      (
                                                       ([alkhalidiah].[dbo].[periods].period_end >= GETDATE())
                                                      OR
                                                       ([alkhalidiah].[dbo].[periods].period_end IS NULL)
                                                       )", to_string_sql_for_SELECT(period_start));
            }
            string sql_str = string.Format(@"SELECT DISTINCT period_end FROM [alkhalidiah].[dbo].[periods]
                                            JOIN (SELECT 
                                                         [alkhalidiah].[dbo].[links].link_period_id
                                                  FROM 
                                                         [alkhalidiah].[dbo].[links]
                                                  WHERE
                                                         [alkhalidiah].[dbo].[links].link_company_id = {0}
                                                    AND
                                                         [alkhalidiah].[dbo].[links].link_country_id = {1}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_city_id = {2}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_hotel_id = {3}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_specialoffer_id = {4}
                                                    
                                                  GROUP BY 
                                                         [alkhalidiah].[dbo].[links].link_period_id
                                                  )
                                                  AS tmp ON
                                                   tmp.link_period_id = [alkhalidiah].[dbo].[periods].id
                                                  WHERE " + sql_str_1,
                                                          company_id, country_id, city_id, hotel_id, specialoffer_id);
            
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(dr["period_end"].ToString().Replace(" 00:00:00", ""));
                }
                dr.Close();
            }
            return select_result;
        }

        public List<string> select_bookingperiods_start(int company_id, string country_id, string city_id, string hotel_id, string specialoffer_id, string period_begin = null,
                                                                                                                                                    string period_end = null)
        {
            string str_period;

            if (period_begin.Equals("0"))
            {
                if (period_end.Equals("0"))
                {
                    str_period = "";
                }
                else
                {
                    str_period = @"AND
                                (
                                    [alkhalidiah].[dbo].[links].link_period_id 
                                    IN 
                                    (
	                                    SELECT 
                                                id 
                                            FROM 
                                                [alkhalidiah].[dbo].[periods] 
                                            WHERE
                                                [alkhalidiah].[dbo].[periods].period_end {5}       
                                    )
                                )";
                }
            }
            else
            {
                if (period_end.Equals("0"))
                {
                    str_period = @"AND
                                (
                                    [alkhalidiah].[dbo].[links].link_period_id 
                                    IN 
                                    (
	                                    SELECT 
                                                id 
                                            FROM 
                                                [alkhalidiah].[dbo].[periods] 
                                            WHERE
                                                [alkhalidiah].[dbo].[periods].period_begin {5}       
                                    )
                                )";
                }
                else
                {
                    str_period = @"AND
                                (
                                    [alkhalidiah].[dbo].[links].link_period_id 
                                    IN 
                                    (
	                                    SELECT 
                                                id 
                                            FROM 
                                                [alkhalidiah].[dbo].[periods] 
                                            WHERE
                                                [alkhalidiah].[dbo].[periods].period_begin {5}
                                            AND 
                                                [alkhalidiah].[dbo].[periods].period_end {6}       
                                    )
                                )";
                }
 
            }
            

            List<string> select_result = new List<string>();
            string sql_str = string.Format(@"SELECT DISTINCT bookingperiod_begin FROM [alkhalidiah].[dbo].[bookingperiods]
                                            JOIN (SELECT 
                                                         [alkhalidiah].[dbo].[links].link_bookingperiod_id
                                                  FROM 
                                                         [alkhalidiah].[dbo].[links]
                                                  WHERE
                                                         [alkhalidiah].[dbo].[links].link_company_id = {0}
                                                    AND
                                                         [alkhalidiah].[dbo].[links].link_country_id = {1}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_city_id = {2}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_hotel_id = {3}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_specialoffer_id = {4} "
                                                    + str_period +
                                                  @" GROUP BY 
                                                         [alkhalidiah].[dbo].[links].link_bookingperiod_id
                                                  )
                                                  AS tmp ON
                                                      tmp.link_bookingperiod_id = [alkhalidiah].[dbo].[bookingperiods].id
                                                   WHERE 
                                                        [alkhalidiah].[dbo].[bookingperiods].bookingperiod_end >= GETDATE()
                                                     OR
                                                        ([alkhalidiah].[dbo].[bookingperiods].bookingperiod_end IS NULL)", company_id, country_id, city_id, hotel_id, specialoffer_id,
                                                                                                                         to_string_sql_for_SELECT(period_begin),
                                                                                                                         to_string_sql_for_SELECT(period_end));
            errors.Add(sql_str);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(dr["bookingperiod_begin"].ToString().Replace(" 00:00:00", ""));
                }
                dr.Close();
            }
            return select_result;
        }

        public List<string> select_bookingperiods_end(int company_id, string country_id, string city_id, string hotel_id, string specialoffer_id, 
                                                                        string period_begin = null, string period_end = null, string bookingperiods_start = null)
        {
            string str_period;
            string str_parametr;

            if (period_begin.Equals("0"))
            {
                if (period_end.Equals("0"))
                {
                    str_period = "";
                    str_parametr = "{5}";
                }
                else
                {
                    str_period = @"AND
                                (
                                    [alkhalidiah].[dbo].[links].link_period_id 
                                    IN 
                                    (
	                                    SELECT 
                                                id 
                                            FROM 
                                                [alkhalidiah].[dbo].[periods] 
                                            WHERE
                                                [alkhalidiah].[dbo].[periods].period_end {5}       
                                    )
                                )";
                    str_parametr = "{6}";
                }
            }
            else
            {
                if (period_end.Equals("0"))
                {
                    str_period = @"AND
                                (
                                    [alkhalidiah].[dbo].[links].link_period_id 
                                    IN 
                                    (
	                                    SELECT 
                                                id 
                                            FROM 
                                                [alkhalidiah].[dbo].[periods] 
                                            WHERE
                                                [alkhalidiah].[dbo].[periods].period_begin {5}       
                                    )
                                )";
                    str_parametr = "{6}";
                }
                else
                {
                    str_period = @"AND
                                (
                                    [alkhalidiah].[dbo].[links].link_period_id 
                                    IN 
                                    (
	                                    SELECT 
                                                id 
                                            FROM 
                                                [alkhalidiah].[dbo].[periods] 
                                            WHERE
                                                [alkhalidiah].[dbo].[periods].period_begin {5}
                                            AND 
                                                [alkhalidiah].[dbo].[periods].period_end {6}       
                                    )
                                )";
                    str_parametr = "{7}";
                }

            }

            
            string str_booking;
            if (bookingperiods_start.Equals("0"))
            {
                str_booking = "";
            }
            else
            {
                str_booking = " [alkhalidiah].[dbo].[bookingperiods].bookingperiod_begin " + str_parametr + " AND ";
            }

            List<string> select_result = new List<string>();
            string sql_str = string.Format(@"SELECT DISTINCT bookingperiod_end FROM [alkhalidiah].[dbo].[bookingperiods]
                                            JOIN (SELECT 
                                                         [alkhalidiah].[dbo].[links].link_bookingperiod_id
                                                  FROM 
                                                         [alkhalidiah].[dbo].[links]
                                                  WHERE
                                                         [alkhalidiah].[dbo].[links].link_company_id = {0}
                                                    AND
                                                         [alkhalidiah].[dbo].[links].link_country_id = {1}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_city_id = {2}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_hotel_id = {3}
                                                    AND  
                                                         [alkhalidiah].[dbo].[links].link_specialoffer_id = {4}
                                                     " + str_period +
                                                  @" GROUP BY 
                                                         [alkhalidiah].[dbo].[links].link_bookingperiod_id
                                                  )
                                                  AS tmp ON
                                                   tmp.link_bookingperiod_id = [alkhalidiah].[dbo].[bookingperiods].id
                                                  WHERE" + str_booking + 
                                                      
                                                         @"(
                                                          [alkhalidiah].[dbo].[bookingperiods].bookingperiod_end >= GETDATE()
                                                          OR
                                                          ([alkhalidiah].[dbo].[bookingperiods].bookingperiod_end IS NULL)
                                                         )", 
                                                           company_id, country_id, city_id, hotel_id, specialoffer_id,
                                                           to_string_sql_for_SELECT(period_begin),
                                                           to_string_sql_for_SELECT(period_end),
                                                           to_string_sql_for_SELECT(bookingperiods_start));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(dr["bookingperiod_end"].ToString().Replace(" 00:00:00", ""));
                }
                dr.Close();
            }
            return select_result;
        }

        public List<string[]> select_abbr(int company_id)
        {
            List<string[]> select_result = new List<string[]>();
            string sql_str = string.Format(@"SELECT TOP 1000 [abbr_code],
                                                             [abbr_alter_code],
                                                             [abbr_priority]
                                                          FROM [alkhalidiah].[dbo].[abbr]
                                                          WHERE [abbr_company_id] = {0}", company_id);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    select_result.Add(new string[] { dr["abbr_code"].ToString(), dr["abbr_alter_code"].ToString(), dr["abbr_priority"].ToString() });
                }
                dr.Close();
            }
            return select_result;
        }

                                                                                         //<abbr
        
        
        //Работа с таблицей bookingperiods                                                                bookingperiods >
        public int insert_bookingperiod(DateTime bookingperiod_begin, DateTime bookingperiod_end)
        {
            int result_id = 0;
            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[bookingperiods]" +
                                           "([bookingperiod_begin], [bookingperiod_end]) OUTPUT INSERTED.ID VALUES" +
                                           "({0}, {1})", to_string_sql(bookingperiod_begin), 
                                                         to_string_sql(bookingperiod_end));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlParameter sqlpr = sql_cmd.Parameters.Add("@ID", SqlDbType.Int);
                sqlpr.Direction = ParameterDirection.Output;
                result_id = (int)sql_cmd.ExecuteScalar();
            }
            return result_id;
        }

        public int select_bookingperiod(DateTime bookingperiod_begin, DateTime bookingperiod_end)
        {
            int result_id = 0;
            string sql_str = string.Format("SELECT [id] FROM [alkhalidiah].[dbo].[bookingperiods]" +
                                              "WHERE [bookingperiod_begin] {0} AND [bookingperiod_end] {1}",
                                                         to_string_sql_for_SELECT(bookingperiod_begin),
                                                         to_string_sql_for_SELECT(bookingperiod_end));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    result_id = (int)dr["id"]; 
                }
                dr.Close();
            }
            return result_id;
        }

       
        public void delete_bookingperiod(int id)
        {
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[bookingperiods]" +
                                           "WHERE id = '{0}'", id);
                                           
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }
                                                                                                           //<bookingperiods

        //Работа с таблицей cities                                                                                cities >
        public int insert_city(string city_code, string city_name)
        {
            int result_id = 0;

            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[cities]" +
                                               "([city_code], [city_name]) OUTPUT INSERTED.ID VALUES" +
                                               "({0}, {1})", to_string_sql(city_code), "@param_cityname");
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
               
                SqlParameter sqlpr = sql_cmd.Parameters.Add("@ID", SqlDbType.Int);
                sql_cmd.Parameters.Add("@param_cityname", SqlDbType.VarChar).Value = city_name;
                sqlpr.Direction = ParameterDirection.Output;
                result_id = (int)sql_cmd.ExecuteScalar();
            }
            return result_id;
        }

        

        public int select_city(string city_code, string city_name)
        {
            int result_id = 0;
            string sql_str = string.Format("SELECT [id] FROM [alkhalidiah].[dbo].[cities]" +
                                              "WHERE [city_code] {0} AND [city_name] {1}",
                                              to_string_sql_for_SELECT(city_code), " = @param_cityname");
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.Parameters.Add("@param_cityname", SqlDbType.VarChar).Value = city_name;
                SqlDataReader dr = sql_cmd.ExecuteReader();
                
                while (dr.Read())
                {
                    result_id = (int)dr["id"];
                }
                dr.Close();
            }
            return result_id;
        }
        public int delete_city(int id)
        {
            int result_id = 0;
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[cities]" +
                                           "WHERE id = '{0}'", id);

            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
            return result_id;
        }
                                                                                                                  //<cities

        //Работа с таблицей hotels                                                                                     hotels >
        public int insert_hotel(string hotel_code, string hotel_name, string hotel_category_name)
        {
            int result_id = 0;
            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[hotels]" +
                                           "([hotel_code], [hotel_name], [hotel_category_name]) OUTPUT INSERTED.ID VALUES" +
                                           "({0}, {1}, {2})", to_string_sql(hotel_code), 
                                                              "@param_hotelname",//to_string_sql(hotel_name), 
                                                              to_string_sql(hotel_category_name));
            
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlParameter sqlpr = sql_cmd.Parameters.Add("@ID", SqlDbType.Int);
                sql_cmd.Parameters.Add("@param_hotelname", SqlDbType.VarChar).Value = hotel_name;
                sqlpr.Direction = ParameterDirection.Output;
                result_id = (int)sql_cmd.ExecuteScalar();
            }
            return result_id;
        }

        public int select_hotel(string hotel_code, string hotel_name, string hotel_category_name)
        {
            int result_id = 0;
            string sql_str = string.Format("SELECT [id] FROM [alkhalidiah].[dbo].[hotels]" +
                                              "WHERE [hotel_code] {0} AND [hotel_name] {1} AND [hotel_category_name] {2}",
                                              to_string_sql_for_SELECT(hotel_code), 
                                              " = @param_hotelname",//to_string_sql_for_SELECT(hotel_name), 
                                              to_string_sql_for_SELECT(hotel_category_name));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.Parameters.Add("@param_hotelname", SqlDbType.VarChar).Value = hotel_name;
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    result_id = (int)dr["id"];
                }
                dr.Close();
            }
            return result_id;
        }
        public int delete_hotel(int id)
        {
            int result_id = 0;
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[hotels]" +
                                           "WHERE id = '{0}'", id);

            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }

            return result_id;
        }
                                                                                                                      //<hotels

        //Работа с таблицей mealtypes                                                                                      mealtypes >
        public int insert_mealtype(string mealtype_code, string mealtype_name)
        {
            int result_id = 0;
            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[mealtypes]" +
                                           "([mealtype_code], [mealtype_name]) OUTPUT INSERTED.ID VALUES" +
                                           "({0}, {1})", to_string_sql(mealtype_code), 
                                                         to_string_sql(mealtype_name));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlParameter sqlpr = sql_cmd.Parameters.Add("@ID", SqlDbType.Int);
                sqlpr.Direction = ParameterDirection.Output;
                result_id = (int)sql_cmd.ExecuteScalar();
            }
            return result_id;
        }

        public int select_mealtype(string mealtype_code, string mealtype_name)
        {
            int result_id = 0;
            string sql_str = string.Format("SELECT [id] FROM [alkhalidiah].[dbo].[mealtypes]" +
                                              "WHERE [mealtype_code] {0} AND [mealtype_name] {1}",
                                              to_string_sql_for_SELECT(mealtype_code),
                                              to_string_sql_for_SELECT(mealtype_name));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    result_id = (int)dr["id"];
                }
                dr.Close();
            }
            return result_id;
        }

        public void delete_mealtype(int id)
        {
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[mealtypes]" +
                                           "WHERE id = '{0}'", id);

            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }
                                                                                                                          //<mealtypes

        //Работа с таблицей periods                                                                                            periods >
        public int insert_period(DateTime period_begin, DateTime period_end)
        {
            int result_id = 0;
            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[periods]" +
                                           "([period_begin], [period_end]) OUTPUT INSERTED.ID VALUES" +
                                           "({0}, {1})", to_string_sql(period_begin), 
                                                         to_string_sql(period_end));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlParameter sqlpr = sql_cmd.Parameters.Add("@ID", SqlDbType.Int);
                sqlpr.Direction = ParameterDirection.Output;
                result_id = (int)sql_cmd.ExecuteScalar();
            }
            return result_id;
        }

        public int select_period(DateTime period_begin, DateTime period_end)
        {
            int result_id = 0;
            string sql_str = string.Format("SELECT [id] FROM [alkhalidiah].[dbo].[periods]" +
                                              "WHERE [period_begin] {0} AND [period_end] {1}",
                                              to_string_sql_for_SELECT(period_begin),
                                              to_string_sql_for_SELECT(period_end));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    result_id = (int)dr["id"];
                }
                dr.Close();
            }
            return result_id;
        }

        public void delete_period(int id)
        {
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[periods]" +
                                           "WHERE id = '{0}'", id);

            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }
                                                                                                                              //<periods

        //Работа с таблицей prices                                                                                                 prices >
        public int insert_price(DateTime price_created, decimal price_amount, string price_currency, decimal price_minstay, 
                                  decimal price_maxstay, string price_type)
        {
            int result_id = 0;
            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[prices]" +
                                           "([price_created], [price_amount], [price_currency]," +
                                           " [price_minstay], [price_maxstay], [price_type]) OUTPUT INSERTED.ID VALUES" +
                                           "({0}, '{1}', {2}, {3}, {4}, {5})", 
                                           to_string_sql(price_created.ToString()), 
                                           price_amount, 
                                           to_string_sql(price_currency), 
                                           to_string_sql(price_minstay.ToString()), 
                                           to_string_sql(price_maxstay.ToString()), 
                                           to_string_sql(price_type));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlParameter sqlpr = sql_cmd.Parameters.Add("@ID", SqlDbType.Int);
                sqlpr.Direction = ParameterDirection.Output;
                result_id = (int)sql_cmd.ExecuteScalar();
            }
            return result_id;
        }

        public void delete_price(int id)
        {
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[prices]" +
                                           "WHERE id = '{0}'", id);

            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }
                                                                                                                                  //<prices

        //Работа с таблицей rooms                                                                                                      rooms >
        public int insert_room(string room_accom_code, string room_accom_name, string room_category_code, string room_category_name,
                                 string room_type_code, string room_type_name)
        {
            int result_id = 0;
            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[rooms]" +
                                           "([room_accomodation_code], [room_accomodation_name], [room_category_code], " +
                                           "[room_category_name], [room_type_code], [room_type_name]) OUTPUT INSERTED.ID VALUES" +
                                           "({0}, {1}, {2}, {3}, {4}, {5})",
                                           to_string_sql(room_accom_code),
                                           to_string_sql(room_accom_name), 
                                           to_string_sql(room_category_code), 
                                           "@room_category_name",
                                           to_string_sql(room_type_code), 
                                           to_string_sql(room_type_name));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlParameter sqlpr = sql_cmd.Parameters.Add("@ID", SqlDbType.Int);
                sql_cmd.Parameters.Add("@room_category_name", SqlDbType.VarChar).Value = room_category_name;
                sqlpr.Direction = ParameterDirection.Output;
                result_id = (int)sql_cmd.ExecuteScalar();
            }
            return result_id;
        }

        public int select_room(string room_accom_code, string room_accom_name, string room_category_code, string room_category_name,
                                 string room_type_code, string room_type_name)
        {
            int result_id = 0;
            string sql_str = string.Format("SELECT [id] FROM [alkhalidiah].[dbo].[rooms]" +
                                              "WHERE [room_accomodation_code] {0} AND " +
                                               "[room_accomodation_name] {1} AND " +
                                               "[room_category_code] {2} AND " +
                                               "[room_category_name] {3} AND " +
                                               "[room_type_code] {4} AND " +
                                               "[room_type_name] {5}",
                                               to_string_sql_for_SELECT(room_accom_code),
                                               to_string_sql_for_SELECT(room_accom_name),
                                               to_string_sql_for_SELECT(room_category_code),
                                               " = @room_category_name",
                                               to_string_sql_for_SELECT(room_type_code),
                                               to_string_sql_for_SELECT(room_type_name));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.Parameters.Add("@room_category_name", SqlDbType.VarChar).Value = room_category_name;
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    result_id = (int)dr["id"];
                }
                dr.Close();
            }
            return result_id;
        }

        public void delete_room(int id)
        {
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[rooms]" +
                                           "WHERE id = '{0}'", id);

            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }
                                                                                                                                     //<rooms

        //Работа с таблицей specialoffers                                                                                                specialoffers >
        public int insert_specialoffer(string specoffer_name, string specoffer_code, int specoffer_nights,
                                         string specoffer_specoffertype_code, string specoffer_specoffertype_name)
        {
            int result_id = 0;
            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[specialoffers]" +
                                           "([specialoffer_name], [specialoffer_code], [specialoffer_nights]," +
                                           "[specialoffer_specialoffertype_code], [specialoffer_specialoffertype_name]) OUTPUT INSERTED.ID VALUES" +
                                           "({0}, {1}, {2}, {3}, {4})",
                                           to_string_sql(specoffer_name),
                                           "@specialoffer_code", 
                                           to_string_sql(specoffer_nights.ToString()), 
                                           to_string_sql(specoffer_specoffertype_code),
                                           to_string_sql(specoffer_specoffertype_name));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlParameter sqlpr = sql_cmd.Parameters.Add("@ID", SqlDbType.Int);
                sql_cmd.Parameters.Add("@specialoffer_code", SqlDbType.VarChar).Value = specoffer_code;
                sqlpr.Direction = ParameterDirection.Output;
                result_id = (int)sql_cmd.ExecuteScalar();
            }
            return result_id;
        }

        public int select_specialoffer(string specoffer_name, string specoffer_code, int specoffer_nights,
                                         string specoffer_specoffertype_code, string specoffer_specoffertype_name)
        {
            int result_id = 0;
            string sql_str = string.Format("SELECT [id] FROM [alkhalidiah].[dbo].[specialoffers]" +
                                              "WHERE [specialoffer_name] {0} AND " +
                                               "[specialoffer_code] {1} AND " +
                                               "[specialoffer_nights] {2} AND " +
                                               "[specialoffer_specialoffertype_code] {3} AND " +
                                               "[specialoffer_specialoffertype_name] {4}",
                                               to_string_sql_for_SELECT(specoffer_name),
                                               " = @specialoffer_code",
                                               to_string_sql_for_SELECT(specoffer_nights.ToString()),
                                               to_string_sql_for_SELECT(specoffer_specoffertype_code),
                                               to_string_sql_for_SELECT(specoffer_specoffertype_name));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.Parameters.Add("@specialoffer_code", SqlDbType.VarChar).Value = specoffer_code;
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    result_id = (int)dr["id"];
                }
                dr.Close();
            }
            return result_id;
        }


        public void delete_specialoffer(int id)
        {
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[specialoffers]" +
                                           "WHERE id = '{0}'", id);

            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }
                                                                                                                                         //<specialoffers

        //Работа с таблицей companies                                                                                                         companies >
        public int insert_company(string company_name, string company_description)
        {
            int result_id = 0;
            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[companies]" +
                                           "([company_name], [company_description]) OUTPUT INSERTED.ID VALUES" +
                                           "({0}, {1})", to_string_sql(company_name), 
                                                         to_string_sql(company_description));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlParameter sqlpr = sql_cmd.Parameters.Add("@ID", SqlDbType.Int);
                sqlpr.Direction = ParameterDirection.Output;
                result_id = (int)sql_cmd.ExecuteScalar();
            }
            return result_id;
        }

        public void update_company_last_id(int company_last_id, int id)
        {
            string sql_str = string.Format("UPDATE [alkhalidiah].[dbo].[companies]" +
                                           "SET [company_last_id] = '{0}'" +
                                           "WHERE [id] = '{1}'", company_last_id, id);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }

        public int select_company_last_id(int id)
        {
            int result_company_last_id = 0;
            string sql_str = string.Format("SELECT [company_last_id] FROM [alkhalidiah].[dbo].[companies]" +
                                              "WHERE [id] = '{0}'",
                                              id);
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    result_company_last_id = (int)dr["company_last_id"];
                }
                dr.Close();
            }
            return result_company_last_id;
        }




        public void delete_company(int id)
        {
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[companies]" +
                                           "WHERE id = '{0}'", id);

            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }
                                                                                                                                             //<companies

        //Работа с таблицей links                                                                                                                    links >
        public void insert_link(int link_hotel_id, int link_city_id, int link_room_id, int link_price_id, int link_mealtype_id,
                                 int link_bookingperiod, int link_period_id, int link_specialoffer_id, int link_company_id, int link_country_id)
        {
            string sql_str = string.Format("INSERT INTO [alkhalidiah].[dbo].[links]" +
                                           "([link_hotel_id], [link_city_id], [link_room_id], [link_price_id], " +
                                           "[link_mealtype_id], [link_bookingperiod_id], [link_period_id], " +
                                           "[link_specialoffer_id], [link_company_id], [link_country_id]) VALUES" +
                                           "({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})", 
                                           to_string_sql(link_hotel_id.ToString()),
                                           to_string_sql(link_city_id.ToString()), 
                                           to_string_sql(link_room_id.ToString()), 
                                           to_string_sql(link_price_id.ToString()), 
                                           to_string_sql(link_mealtype_id.ToString()),
                                           to_string_sql(link_bookingperiod.ToString()), 
                                           to_string_sql(link_period_id.ToString()), 
                                           to_string_sql(link_specialoffer_id.ToString()), 
                                           to_string_sql(link_company_id.ToString()),
                                           to_string_sql(link_country_id.ToString()));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }

        public int select_link(int link_hotel_id, int link_city_id, int link_room_id, int link_price_id, int link_mealtype_id,
                                 int link_bookingperiod, int link_period_id, int link_specialoffer_id, int link_company_id)
        {
            int result_id = 0;
            string sql_str = string.Format("SELECT [id] FROM [alkhalidiah].[dbo].[specialoffers]" +
                                              "WHERE [link_hotel_id] {0} AND " +
                                              "[link_city_id] {1} AND " +
                                               "[link_room_id] {2} AND " +
                                               "[link_price_id] {3} AND " +
                                               "[link_mealtype_id] {4} AND " +
                                               "[link_bookingperiod_id] {5} AND " +
                                               "[link_period_id] {6}",
                                               to_string_sql_for_SELECT(link_hotel_id.ToString()),
                                               to_string_sql_for_SELECT(link_city_id.ToString()),
                                               to_string_sql_for_SELECT(link_room_id.ToString()),
                                               to_string_sql_for_SELECT(link_price_id.ToString()),
                                               to_string_sql_for_SELECT(link_mealtype_id.ToString()),
                                               to_string_sql_for_SELECT(link_bookingperiod.ToString()),
                                               to_string_sql_for_SELECT(link_period_id.ToString()),
                                               to_string_sql_for_SELECT(link_specialoffer_id.ToString()),
                                               to_string_sql_for_SELECT(link_company_id.ToString()));
            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                SqlDataReader dr = sql_cmd.ExecuteReader();
                while (dr.Read())
                {
                    result_id = (int)dr["id"];
                }
                dr.Close();
            }
            return result_id;
        }

        public void delete_link(int id)
        {
            string sql_str = string.Format("DELETE FROM [alkhalidiah].[dbo].[links]" +
                                           "WHERE id = '{0}'", id);

            using (SqlCommand sql_cmd = new SqlCommand(sql_str, this.sqlCN))
            {
                sql_cmd.ExecuteNonQuery();
            }
        }

        private string to_string_sql(string value)
        {
            return (value == null||string.Compare(value,"0") == 0) ? "NULL" : "'" + value + "'";
        }
                                                                                                                                                    //<links
        private string to_string_sql(DateTime value)
        {
            return (value < dt_const) ? "NULL" : "'" + value + "'";
        }


        private string to_string_sql_for_SELECT(string value)
        {
            return (value == null || string.Compare(value, "0") == 0) ? "IS NULL" : "= '" + value + "'";
        }

        private string to_string_sql_for_SELECT(DateTime value)
        {
            return (value < dt_const) ? "IS NULL" : "= '" + value + "'";
        }

    }
}
