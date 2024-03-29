var $ = require('jquery');

window.jQuery = $;
var bootstrap = require('bootstrap'); //eslint-disable-line no-unused-vars
var bootbox = require('bootbox');

var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');

var TopologyView = BaseView.extend({
    className: 'topology container-fluid',

    template: require('./spinner.hbs'),

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        var isAdmin = this.model.get('isAdmin');
        this.model.fetch()
            .done(function(data) {
                this.template = require('./topology.hbs');
                this.render({'message': data['message'], 'isAdmin': isAdmin});
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    },
});

module.exports = TopologyView;
