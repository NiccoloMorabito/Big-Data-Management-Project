CREATE TABLE transactions
(
    id               VARCHAR(50) PRIMARY KEY,
    origin           CHAR(3) REFERENCES countries (iso3),
    destination      CHAR(3) REFERENCES countries (iso3),
    transaction_date DATE,
    price            FLOAT,
    unit             VARCHAR,
    quantity         FLOAT,
    product_category CHAR(4) REFERENCES categories (subcategory_code),
    description      VARCHAR
);

CREATE VIEW agricultural_exports_agg AS
(
SELECT to_char(transaction_date, 'YYYY-MM') as month,
       subcategory_code                     as category,
       origin,
       sum(price)                           as total_price
from transactions
         JOIN categories c ON transactions.product_category = c.subcategory_code
WHERE supercategory_code = 'II'
GROUP BY subcategory_code,
         month,
         origin
    );


CREATE MATERIALIZED VIEW agricultural_exports_agg_full AS
(
WITH cats AS (SELECT DISTINCT category
              FROM agricultural_exports_agg),
     month_cat_count AS (SELECT month, category, iso3
                         FROM months,
                              cats,
                              countries)
SELECT month_cat_count.month      AS month,
       month_cat_count.category   AS category,
       COALESCE(total_price, 0.0) AS total_price,
       month_cat_count.iso3       AS origin
FROM month_cat_count
         LEFT OUTER JOIN agricultural_exports_agg aea
                         ON month_cat_count.month = aea.month
                             AND month_cat_count.category = aea.category
                             AND month_cat_count.iso3 = aea.origin
    );

CREATE TABLE detailed_categories
(
    detail_code        CHAR(6) PRIMARY KEY,
    detail_por         VARCHAR(500),
    detail_esp         VARCHAR(500),
    detail_eng         VARCHAR(500),
    subcategory_code   CHAR(4),
    subcategory_por    VARCHAR(500),
    subcategory_esp    VARCHAR(500),
    subcategory_eng    VARCHAR(500),
    category_code      CHAR(2),
    category_por       VARCHAR(500),
    category_esp       VARCHAR(500),
    category_eng       VARCHAR(500),
    supercategory_code VARCHAR(10),
    supercategory_por  VARCHAR(500),
    supercategory_esp  VARCHAR(500),
    supercategory_eng  VARCHAR(500)
);

CREATE TABLE categories
(
    subcategory_code   CHAR(4) PRIMARY KEY,
    subcategory_por    VARCHAR(500),
    subcategory_esp    VARCHAR(500),
    subcategory_eng    VARCHAR(500),
    category_code      CHAR(2),
    category_por       VARCHAR(500),
    category_esp       VARCHAR(500),
    category_eng       VARCHAR(500),
    supercategory_code VARCHAR(10),
    supercategory_por  VARCHAR(500),
    supercategory_esp  VARCHAR(500),
    supercategory_eng  VARCHAR(500)
);

CREATE TABLE countries
(
    name VARCHAR(100),
    iso2 CHAR(2) UNIQUE,
    iso3 CHAR(3) PRIMARY KEY,
    ison CHAR(3) UNIQUE
);

-- After loading categories

-- Add subcategories
INSERT INTO detailed_categories(SELECT RPAD(subcategory_code, 6, '0'),
                                       subcategory_por,
                                       subcategory_esp,
                                       subcategory_eng,
                                       subcategory_code,
                                       subcategory_por,
                                       subcategory_esp,
                                       subcategory_eng,
                                       category_code,
                                       category_por,
                                       category_esp,
                                       category_eng,
                                       supercategory_code,
                                       supercategory_por,
                                       supercategory_esp,
                                       supercategory_eng
                                FROM detailed_categories
                                GROUP BY subcategory_code,
                                         subcategory_por,
                                         subcategory_esp,
                                         subcategory_eng,
                                         category_code,
                                         category_por,
                                         category_esp,
                                         category_eng,
                                         supercategory_code,
                                         supercategory_por,
                                         supercategory_esp,
                                         supercategory_eng)
ON CONFLICT DO NOTHING;

-- Add categories
INSERT INTO detailed_categories(SELECT RPAD(category_code, 6, '0'),
                                       category_por,
                                       category_esp,
                                       category_eng,
                                       RPAD(category_code, 4, '0'),
                                       category_por,
                                       category_esp,
                                       category_eng,
                                       category_code,
                                       category_por,
                                       category_esp,
                                       category_eng,
                                       supercategory_code,
                                       supercategory_por,
                                       supercategory_esp,
                                       supercategory_eng
                                FROM detailed_categories
                                GROUP BY category_code,
                                         category_por,
                                         category_esp,
                                         category_eng,
                                         supercategory_code,
                                         supercategory_por,
                                         supercategory_esp,
                                         supercategory_eng)
ON CONFLICT DO NOTHING;


INSERT INTO categories (SELECT subcategory_code,
                               subcategory_por,
                               subcategory_esp,
                               subcategory_eng,
                               category_code,
                               category_por,
                               category_esp,
                               category_eng,
                               supercategory_code,
                               supercategory_por,
                               supercategory_esp,
                               supercategory_eng
                        FROM detailed_categories
                        GROUP BY subcategory_code,
                                 subcategory_por,
                                 subcategory_esp,
                                 subcategory_eng,
                                 category_code,
                                 category_por,
                                 category_esp,
                                 category_eng,
                                 supercategory_code,
                                 supercategory_por,
                                 supercategory_esp,
                                 supercategory_eng)
