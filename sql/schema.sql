CREATE TABLE publisher (
    id SERIAL NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE book_language (
    id SERIAL NOT NULL PRIMARY KEY,
    language VARCHAR(255) NOT NULL
);

CREATE TABLE book (
    id UUID NOT NULL PRIMARY KEY,
    isbn VARCHAR(17) NOT NULL UNIQUE,
    synopsis TEXT NOT NULL,
    title VARCHAR(255) NOT NULL,
    total_ratings INTEGER NOT NULL DEFAULT 0 CHECK (total_ratings >= 0),
    total_reviews INTEGER NOT NULL DEFAULT 0 CHECK (total_reviews >= 0),
    book_picture VARCHAR(255) NOT NULL,
    total_pages INTEGER CHECK (total_pages > 0) NOT NULL,
    published_year INTEGER CHECK (published_year >= 0) NOT NULL,
    id_language INTEGER NOT NULL,
    id_publisher INTEGER NOT NULL,
    created_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (id_publisher) REFERENCES publisher(id),
    FOREIGN KEY (id_language) REFERENCES book_language(id)
);

CREATE TABLE book_location (
    id SERIAL NOT NULL PRIMARY KEY,
    location_name VARCHAR(255) NOT NULL,
    location GEOGRAPHY(Point, 4326) NOT NULL,
    id_book UUID NOT NULL,
    FOREIGN KEY (id_book) REFERENCES book(id)
);

CREATE TABLE author (
    id SERIAL NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE authored_by (
    id_book UUID NOT NULL,
    id_author INTEGER NOT NULL,
    PRIMARY KEY (id_book, id_author),
    FOREIGN KEY (id_book) REFERENCES book(id),
    FOREIGN KEY (id_author) REFERENCES author(id)
);

CREATE TABLE genre (
    id SERIAL NOT NULL PRIMARY KEY,
    genre VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE having_genre (
    id_book UUID NOT NULL,
    id_genre INTEGER NOT NULL,
    PRIMARY KEY (id_book, id_genre),
    FOREIGN KEY (id_book) REFERENCES book(id),
    FOREIGN KEY (id_genre) REFERENCES genre(id)
);

CREATE TABLE review (
    id_user UUID NOT NULL,
    message TEXT NOT NULL,
    rating INTEGER NOT NULL DEFAULT 0 CHECK (rating >= 0 AND rating <= 5),
    id_book UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (id_user, id_book),
    FOREIGN KEY (id_book) REFERENCES book(id)
);

CREATE TABLE having_user_book (
    id_user UUID NOT NULL,
    id_book UUID NOT NULL,
    PRIMARY KEY (id_user, id_book),
    FOREIGN KEY (id_book) REFERENCES book(id)
);
