import logging
import json

from django.db.models.sql.compiler import SQLCompiler as BaseSQLCompiler
from django.db.utils import DatabaseError
from djangotoolbox.db.basecompiler import NonrelQuery, NonrelCompiler, \
    NonrelInsertCompiler, NonrelUpdateCompiler, NonrelDeleteCompiler
from django.db.models.fields import AutoField

import datetime

import django
from django.conf import settings
from django.db.models.fields import NOT_PROVIDED
from django.db.models.query import QuerySet
from django.db.models.sql import aggregates as sqlaggregates
from django.db.models.sql.constants import MULTI, SINGLE
from django.db.models.sql.where import AND, OR
from django.db.utils import DatabaseError, IntegrityError
from django.utils.tree import Node
from django.db import connections

try:
    from django.db.models.sql.where import SubqueryConstraint
except ImportError:
    SubqueryConstraint = None

try:
    from django.db.models.sql.datastructures import EmptyResultSet
except ImportError:
    class EmptyResultSet(Exception):
        pass


if django.VERSION >= (1, 5):
    from django.db.models.constants import LOOKUP_SEP
else:
    from django.db.models.sql.constants import LOOKUP_SEP

if django.VERSION >= (1, 6):
    def get_selected_fields(query):
        if query.select:
            return [info.field for info in (query.select +
                        query.related_select_cols)]
        else:
            return query.model._meta.fields
else:
    def get_selected_fields(query):
        if query.select_fields:
            return (query.select_fields + query.related_select_fields)
        else:
            return query.model._meta.fields


from django_elasticsearch import WRITE_QUEUE

__author__ = 'jorgealegre'

logger = logging.getLogger(__name__)


class DBQuery(NonrelQuery):

    def __init__(self, compiler, fields):
        super(DBQuery, self).__init__(compiler, fields)

    def fetch(self, low_mark=0, high_mark=None):
        """
        Returns an iterator over some part of query results.
        """
        raise NotImplementedError

    def count(self, limit=None):
        """
        Returns the number of objects that would be returned, if
        this query was executed, up to `limit`.
        """
        raise NotImplementedError

    def delete(self):
        """
        Called by NonrelDeleteCompiler after it builds a delete query.
        """
        raise NotImplementedError

    def order_by(self, ordering):
        """
        Reorders query results or execution order. Called by
        NonrelCompilers during query building.

        :param ordering: A list with (field, ascending) tuples or a
                         boolean -- use natural ordering, if any, when
                         the argument is True and its reverse otherwise
        """
        raise NotImplementedError

    def add_filter(self, field, lookup_type, negated, value):
        """
        Adds a single constraint to the query. Called by add_filters for
        each constraint leaf in the WHERE tree built by Django.

        :param field: Lookup field (instance of Field); field.column
                      should be used for database keys
        :param lookup_type: Lookup name (e.g. "startswith")
        :param negated: Is the leaf negated
        :param value: Lookup argument, such as a value to compare with;
                      already prepared for the database
        """
        raise NotImplementedError

    def add_filters(self, filters):
        """
        Converts a constraint tree (sql.where.WhereNode) created by
        Django's SQL query machinery to nonrel style filters, calling
        add_filter for each constraint.

        This assumes the database doesn't support alternatives of
        constraints, you should override this method if it does.

        TODO: Simulate both conjunctions and alternatives in general
              let GAE override conjunctions not to split them into
              multiple queries.
        """
        if filters.negated:
            self._negated = not self._negated

        if not self._negated and filters.connector != AND:
            raise DatabaseError("Only AND filters are supported.")

        # Remove unneeded children from the tree.
        children = self._get_children(filters.children)

        if self._negated and filters.connector != OR and len(children) > 1:
            raise DatabaseError("When negating a whole filter subgroup "
                                "(e.g. a Q object) the subgroup filters must "
                                "be connected via OR, so the non-relational "
                                "backend can convert them like this: "
                                "'not (a OR b) => (not a) AND (not b)'.")

        # Recursively call the method for internal tree nodes, add a
        # filter for each leaf.
        for child in children:
            if isinstance(child, Node):
                self.add_filters(child)
                continue
            field, lookup_type, value = self._decode_child(child)
            self.add_filter(field, lookup_type, self._negated, value)

        if filters.negated:
            self._negated = not self._negated

    # ----------------------------------------------
    # Internal API for reuse by subclasses
    # ----------------------------------------------

    def _decode_child(self, child):
        """
        Produces arguments suitable for add_filter from a WHERE tree
        leaf (a tuple).
        """

        # TODO: Call get_db_prep_lookup directly, constraint.process
        #       doesn't do much more.
        constraint, lookup_type, annotation, value = child
        packed, value = constraint.process(lookup_type, value, self.connection)
        alias, column, db_type = packed
        field = constraint.field

        opts = self.query.model._meta
        if alias and alias != opts.db_table:
            raise DatabaseError("This database doesn't support JOINs "
                                "and multi-table inheritance.")

        # For parent.child_set queries the field held by the constraint
        # is the parent's primary key, while the field the filter
        # should consider is the child's foreign key field.
        if column != field.column:
            if not field.primary_key:
                raise DatabaseError("This database doesn't support filtering "
                                    "on non-primary key ForeignKey fields.")

            field = (f for f in opts.fields if f.column == column).next()
            assert field.rel is not None

        value = self._normalize_lookup_value(
            lookup_type, value, field, annotation)

        return field, lookup_type, value

    def _normalize_lookup_value(self, lookup_type, value, field, annotation):
        """
        Undoes preparations done by `Field.get_db_prep_lookup` not
        suitable for nonrel back-ends and passes the lookup argument
        through nonrel's `value_for_db`.

        TODO: Blank `Field.get_db_prep_lookup` and remove this method.
        """

        # Undo Field.get_db_prep_lookup putting most values in a list
        # (a subclass may override this, so check if it's a list) and
        # losing the (True / False) argument to the "isnull" lookup.
        if lookup_type not in ('in', 'range', 'year') and \
           isinstance(value, (tuple, list)):
            if len(value) > 1:
                raise DatabaseError("Filter lookup type was %s; expected the "
                                    "filter argument not to be a list. Only "
                                    "'in'-filters can be used with lists." %
                                    lookup_type)
            elif lookup_type == 'isnull':
                value = annotation
            else:
                value = value[0]

        # Remove percents added by Field.get_db_prep_lookup (useful
        # if one were to use the value in a LIKE expression).
        if lookup_type in ('startswith', 'istartswith'):
            value = value[:-1]
        elif lookup_type in ('endswith', 'iendswith'):
            value = value[1:]
        elif lookup_type in ('contains', 'icontains'):
            value = value[1:-1]

        # Prepare the value for a database using the nonrel framework.
        return self.ops.value_for_db(value, field, lookup_type)

    def _get_children(self, children):
        """
        Filters out nodes of the given contraint tree not needed for
        nonrel queries; checks that given constraints are supported.
        """
        result = []
        for child in children:

            if SubqueryConstraint is not None \
              and isinstance(child, SubqueryConstraint):
                raise DatabaseError("Subqueries are not supported.")

            if isinstance(child, tuple):
                constraint, lookup_type, _, value = child

                # When doing a lookup using a QuerySet Django would use
                # a subquery, but this won't work for nonrel.
                # TODO: Add a supports_subqueries feature and let
                #       Django evaluate subqueries instead of passing
                #       them as SQL strings (QueryWrappers) to
                #       filtering.
                if isinstance(value, QuerySet):
                    raise DatabaseError("Subqueries are not supported.")

                # Remove leafs that were automatically added by
                # sql.Query.add_filter to handle negations of outer
                # joins.
                if lookup_type == 'isnull' and constraint.field is None:
                    continue

            result.append(child)
        return result

    def _matches_filters(self, entity, filters):
        """
        Checks if an entity returned by the database satisfies
        constraints in a WHERE tree (in-memory filtering).
        """

        # Filters without rules match everything.
        if not filters.children:
            return True

        result = filters.connector == AND

        for child in filters.children:

            # Recursively check a subtree,
            if isinstance(child, Node):
                submatch = self._matches_filters(entity, child)

            # Check constraint leaf, emulating a database condition.
            else:
                field, lookup_type, lookup_value = self._decode_child(child)
                entity_value = entity[field.column]

                if entity_value is None:
                    if isinstance(lookup_value, (datetime.datetime, datetime.date,
                                          datetime.time)):
                        submatch = lookup_type in ('lt', 'lte')
                    elif lookup_type in (
                            'startswith', 'contains', 'endswith', 'iexact',
                            'istartswith', 'icontains', 'iendswith'):
                        submatch = False
                    else:
                        submatch = EMULATED_OPS[lookup_type](
                            entity_value, lookup_value)
                else:
                    submatch = EMULATED_OPS[lookup_type](
                        entity_value, lookup_value)

            if filters.connector == OR and submatch:
                result = True
                break
            elif filters.connector == AND and not submatch:
                result = False
                break

        if filters.negated:
            return not result
        return result

    def _order_in_memory(self, lhs, rhs):
        for field, ascending in self.compiler._get_ordering():
            column = field.column
            result = cmp(lhs.get(column), rhs.get(column))
            if result != 0:
                return result if ascending else -result
        return 0


class SQLCompiler(NonrelCompiler):
    """
    A simple query: no joins, no distinct, etc.
    """
    query_class = DBQuery

    def results_iter(self):
        """
        Returns an iterator over the results from executing query given
        to this compiler. Called by QuerySet methods.
        """
        fields = self.get_fields()
        try:
            results = self.build_query(fields).fetch(
                self.query.low_mark, self.query.high_mark)
        except EmptyResultSet:
            results = []

        for entity in results:
            yield self._make_result(entity, fields)

    def has_results(self):
        return self.get_count(check_exists=True)

    def execute_sql(self, result_type=MULTI):
        """
        Handles SQL-like aggregate queries. This class only emulates COUNT
        by using abstract NonrelQuery.count method.
        """
        aggregates = self.query.aggregate_select.values()

        # Simulate a count().
        if aggregates:
            assert len(aggregates) == 1
            aggregate = aggregates[0]
            assert isinstance(aggregate, sqlaggregates.Count)
            opts = self.query.get_meta()
            if aggregate.col != '*' and \
                aggregate.col != (opts.db_table, opts.pk.column):
                raise DatabaseError("This database backend only supports "
                                    "count() queries on the primary key.")

            count = self.get_count()
            if result_type is SINGLE:
                return [count]
            elif result_type is MULTI:
                return [[count]]

        raise NotImplementedError("The database backend only supports "
                                  "count() queries.")

    # ----------------------------------------------
    # Additional NonrelCompiler API
    # ----------------------------------------------

    def _make_result(self, entity, fields):
        """
        Decodes values for the given fields from the database entity.

        The entity is assumed to be a dict using field database column
        names as keys. Decodes values using `value_from_db` as well as
        the standard `convert_values`.
        """
        result = []
        for field in fields:
            value = entity.get(field.column, NOT_PROVIDED)
            if value is NOT_PROVIDED:
                value = field.get_default()
            else:
                value = self.ops.value_from_db(value, field)
                value = self.query.convert_values(value, field,
                                                  self.connection)
            if value is None and not field.null:
                raise IntegrityError("Non-nullable field %s can't be None!" %
                                     field.name)
            result.append(value)
        return result

    def check_query(self):
        """
        Checks if the current query is supported by the database.

        In general, we expect queries requiring JOINs (many-to-many
        relations, abstract model bases, or model spanning filtering),
        using DISTINCT (through `QuerySet.distinct()`, which is not
        required in most situations) or using the SQL-specific
        `QuerySet.extra()` to not work with nonrel back-ends.
        """
        if hasattr(self.query, 'is_empty') and self.query.is_empty():
            raise EmptyResultSet()
        if (len([a for a in self.query.alias_map if
                 self.query.alias_refcount[a]]) > 1 or
            self.query.distinct or self.query.extra or self.query.having):
            raise DatabaseError("This query is not supported by the database.")

    def get_count(self, check_exists=False):
        """
        Counts objects matching the current filters / constraints.

        :param check_exists: Only check if any object matches
        """
        if check_exists:
            high_mark = 1
        else:
            high_mark = self.query.high_mark
        try:
            return self.build_query().count(high_mark)
        except EmptyResultSet:
            return 0

    def build_query(self, fields=None):
        """
        Checks if the underlying SQL query is supported and prepares
        a NonrelQuery to be executed on the database.
        """
        self.check_query()
        if fields is None:
            fields = self.get_fields()
        query = self.query_class(self, fields)
        query.add_filters(self.query.where)
        query.order_by(self._get_ordering())

        # This at least satisfies the most basic unit tests.
        if connections[self.using].use_debug_cursor or (connections[self.using].use_debug_cursor is None and
                                                            settings.DEBUG):
            self.connection.queries.append({'sql': repr(query)})
        return query

    def get_fields(self):
        """
        Returns fields which should get loaded from the back-end by the
        current query.
        """

        # We only set this up here because related_select_fields isn't
        # populated until execute_sql() has been called.
        fields = get_selected_fields(self.query)

        # If the field was deferred, exclude it from being passed
        # into `resolve_columns` because it wasn't selected.
        only_load = self.deferred_to_columns()
        if only_load:
            db_table = self.query.model._meta.db_table
            only_load = dict((k, v) for k, v in only_load.items()
                             if v or k == db_table)
            if len(only_load.keys()) > 1:
                raise DatabaseError("Multi-table inheritance is not "
                                    "supported by non-relational DBs %s." %
                                    repr(only_load))
            fields = [f for f in fields if db_table in only_load and
                      f.column in only_load[db_table]]

        query_model = self.query.model
        if query_model._meta.proxy:
            query_model = query_model._meta.proxy_for_model

        for field in fields:
            if field.model._meta != query_model._meta:
                raise DatabaseError("Multi-table inheritance is not "
                                    "supported by non-relational DBs.")
        return fields

    def _get_ordering(self):
        """
        Returns a list of (field, ascending) tuples that the query
        results should be ordered by. If there is no field ordering
        defined returns just the standard_ordering (a boolean, needed
        for MongoDB "$natural" ordering).
        """
        opts = self.query.get_meta()
        if not self.query.default_ordering:
            ordering = self.query.order_by
        else:
            ordering = self.query.order_by or opts.ordering

        if not ordering:
            return self.query.standard_ordering

        field_ordering = []
        for order in ordering:
            if LOOKUP_SEP in order:
                raise DatabaseError("Ordering can't span tables on "
                                    "non-relational backends (%s)." % order)
            if order == '?':
                raise DatabaseError("Randomized ordering isn't supported by "
                                    "the backend.")

            ascending = not order.startswith('-')
            if not self.query.standard_ordering:
                ascending = not ascending

            name = order.lstrip('+-')
            if name == 'pk':
                name = opts.pk.name

            field_ordering.append((opts.get_field(name), ascending))
        return field_ordering


class SQLInsertCompiler(SQLCompiler):

    def __init__(self, *args, **kwargs):
        super(SQLInsertCompiler, self).__init__(*args, **kwargs)
        self.opts = self.query.get_meta()

    def _get_pk(self, data):
        """
        Get primary key

        How is possible we have pk in doc???

        :param data:
        :return:
        """
        pk_column = self.opts.pk.column
        pk = None
        if pk_column in data:
            pk = data[pk_column]
        return pk

    def _get_internal_data(self):
        """
        Get internal data for insert operation

        :return:
        """
        # TODO: make one query to ES for internal data for model and default indices
        from mapping import model_to_mapping
        data = {
            'indices': {
                'default': [
                    {}
                ],
                'model': {
                    'main': [
                        {}
                    ],
                    'index': [
                        {}
                    ]
                },
            },
            'is_blocked': False,
        }
        # model = self.opts.db_table
        # default_indices = self.connection.default_indices
        # index_data = self.opts.indices[0]
        # indices = ["{}__{}".format(self.opts.db_table, index_data.keys()[0])]
        # also save mapping in case needs to
        indices = data['indices']['default'] + \
            data['indices']['model']['main'] + \
            data['indices']['model']['index']
        for index_data in indices:
            if index_data['has_mapping'] is False:
                try:
                    mapping = model_to_mapping(self.opts.db_table,
                                               self.connection.connection,
                                               index_data['index'])
                    mapping.save()
                except Exception:
                    pass
        return data

    def _send_queue(self, bulk_data):
        """
        Send data to queue, adding to bulk

        :param bulk_data: bulk data to write to queue
        :return:
        """
        import base64
        bulk_data_encoded = base64.encodestring(bulk_data)
        queue_bulk_data = json.dumps({
            u'create': {
                u'_index': self.connection.default_indices[0],
                u'_type': WRITE_QUEUE,
            }
        }) + '\n' + json.dumps({'data': bulk_data_encoded}) + '\n'
        self.connection.connection.bulker.add(queue_bulk_data)

    def execute_sql(self, return_id=False):
        """
        Execute insert statement

        Insert data into ElasticSearch using bulk inserts.

        :param bool return_id:
        :return: primary key saved in case we have return_id True.
        """
        import time
        assert not (return_id and len(self.query.objs) != 1)
        # query internal index to get indices, like 'alias': [index1, index2]
        # alias would be the default indices, model table name
        internal_data = self._get_internal_data()
        while internal_data['is_blocked']:
            time.sleep(0.2)
            internal_data = self._get_internal_data()
        pk_field = self.opts.pk
        for obj in self.query.objs:
            field_values = {}
            for field in self.query.fields:
                field, field_kind, db_type = self.ops.convert_as(field)
                # check field_kind if is related field or many to many
                if field_kind in ['ForeignKey', 'GenericRelation', 'GenericForeignKey']:
                    # we need the model associated with field
                    logger.debug(u'SQLInsertCompiler.execute_sql :: field_kind: {} field: {} rel: {}'.format(
                        field_kind,
                        field.name,
                        field.rel.to
                    ))
                    value = self.ops.to_dict(field.rel.to)
                    logger.debug(u'SQLInsertCompiler.execute_sql :: object :: value: {}'.format(value))
                else:
                    value = field.get_db_prep_save(
                        getattr(obj, field.attname) if self.query.raw else field.pre_save(obj, obj._state.adding),
                        connection=self.connection
                    )
                    if value is None and not field.null and not field.primary_key:
                        raise IntegrityError(u"You can't set {} (a non-nullable field) to None!".format(field.name))

                logger.debug(u'SQLInsertCompiler.execute_sql :: before value_for_db :: field: {} '
                             u'value: {}'.format(field, value))
                value = self.ops.value_for_db(value, field)
                logger.debug(u'SQLInsertCompiler.execute_sql :: after value_for_db :: value: {}'.format(value))
                field_values[field.column] = value
            if not hasattr(self.opts, 'disable_default_index') or \
                    (hasattr(self.opts, 'disable_default_index') and self.opts.disable_default_index is False):
                # default index
                logger.debug(u'SQLInsertCompiler.execute_sql :: default index')
                for index_data in internal_data['indices']['default']:
                    bulk_data = json.dumps({
                        u'create': {
                            u'_index': index,
                            u'_type': self.opts.db_table,
                            u'_id': self._get_pk(field_values),
                        }
                    }) + '\n' + json.dumps(field_values) + '\n'
                    logger.debug(u'SQLInsertCompiler.execute_sql :: default index index: {}'.format(
                        index
                    ))
                    logger.debug(u'SQLInsertCompiler.execute_sql :: bulk obj: {}'.format(bulk_data))
                    if index_data['rebuild_mode'] == 'building':
                        self._send_queue(bulk_data)
                    else:
                        self.connection.connection.bulker.add(bulk_data)
            if hasattr(self.opts, 'indices') and self.opts.indices:
                # custom general index
                logger.debug(u'SQLInsertCompiler.execute_sql :: disable default index')
                index_data = self.opts.indices[0]
                for index in internal_data['indices']['model']['main']:
                    index_conf = {
                        u'create': {
                            u'_index': index,
                            u'_type': self.opts.db_table,
                            u'_id': self._get_pk(field_values),
                        }
                    }
                    if 'routing' in index_data:
                        index_conf.update({
                            u'_routing': index_data['routing']
                        })
                    bulk_data = json.dumps(index_conf) + '\n' + \
                        json.dumps(field_values) + '\n'
                    logger.debug(u'SQLInsertCompiler.execute_sql :: bulk obj: {}'.format(bulk_data))
                    if index_data['rebuild_mode'] == 'building':
                        self._send_queue(bulk_data)
                    else:
                        self.connection.connection.bulker.add(bulk_data)
            # model indices
            if hasattr(self.opts, 'indices') and len(self.opts.indices) > 1:
                for index_data in internal_data['indices']['model']['index']:
                    logger.debug(u'SQLInsertCompiler.execute_sql :: index: {}'.format(index_data.keys()[0]))
                    index = "{}__{}".format(self.opts.db_table, index_data.keys()[0])
                    index_conf = {
                        u'index': {
                            u'_index': index,
                            u'_type': self.opts.db_table,
                            u'_id': self._get_pk(field_values),
                        }
                    }
                    if 'routing' in index_data:
                        index_conf.update({
                            u'_routing': index_data['routing']
                        })
                    bulk_data = json.dumps(index_conf) + '\n' + \
                        json.dumps(field_values) + '\n'
                    logger.debug(u'SQLInsertCompiler.execute_sql :: bulk obj: {}'.format(bulk_data))
                    if index_data['rebuild_mode'] == 'building':
                        self._send_queue(bulk_data)
                    else:
                        self.connection.connection.bulker.add(bulk_data)
        # Writes real inserts into indices as well as dumps into queue (write_queue)
        res = self.connection.connection.bulker.flush_bulk(forced=True)
        # Pass the key value through normal database de-conversion.
        logger.debug(u'SQLInsertCompiler.execute_sql :: response: {} type: {}'.format(res, type(res)))
        if return_id is False:
            return
        # keys = res['items']['create']['_id']
        keys = map(lambda x: x['create']['_id'] if 'create' in x else x['index']['_id'], res['items'])
        logger.debug(u'SQLInsertCompiler.execute_sql :: response keys: {}'.format(keys))
        # curl -XGET 'http://localhost:9200/djes_test/djes_examplemodelmeta/_search?q=*:*&pretty'
        # from djes.models import ExampleModel, ExampleModelMeta
        # ExampleModelMeta.objects.create(name_people='I am the real thing', has_address=True, number_votes=756)
        return self.ops.convert_values(self.ops.value_from_db(keys[0], pk_field), pk_field)


class SQLUpdateCompiler(SQLCompiler):

    def execute_sql(self, return_id=False):
        pass


class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):

    def execute_sql(self, return_id=False):
        pass
