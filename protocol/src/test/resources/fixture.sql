create schema foo;

create table foo.bar(
  id serial primary key,
  quux text
);

insert into foo.bar (quux) values
  ('alpha'), ('bravo'), ('charlie');
