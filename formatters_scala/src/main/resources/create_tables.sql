CREATE TABLE transactions
(
    origin           varchar(60),
    destination      varchar(60),
    transaction_date date,
    price            float,
    unit             varchar,
    quantity         int,
    product_category CHAR(6) REFERENCES categories (subcategory_code),
    description      varchar NULL
);

CREATE TABLE categories
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

-- After loading categories

-- Add subcategories
INSERT INTO categories(SELECT RPAD(subcategory_code, 6, '0'),
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
                       FROM categories
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
INSERT INTO categories(SELECT RPAD(category_code, 6, '0'),
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
                       FROM categories
                       GROUP BY category_code,
                                category_por,
                                category_esp,
                                category_eng,
                                supercategory_code,
                                supercategory_por,
                                supercategory_esp,
                                supercategory_eng)
ON CONFLICT DO NOTHING;

