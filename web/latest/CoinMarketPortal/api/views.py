import logging
import math
import json

from flask import (Blueprint, abort, current_app, flash, redirect, request,
                   url_for)
from flask.views import MethodView
from flask_allows import And, Permission
from flask_babelplus import gettext as _
from flask_login import current_user, login_required
from pluggy import HookimplMarker
from sqlalchemy import asc, desc

from flaskbb.extensions import allows, db
from flaskbb.forum.forms import (NewTopicForm, QuickreplyForm, ReplyForm,
                                 ReportForm, SearchPageForm, UserSearchForm)
from flaskbb.forum.models import (Category, Forum, ForumsRead, Post, Topic,
                                  TopicsRead)
from flaskbb.user.models import User
from flaskbb.utils.helpers import (do_topic_action, format_quote,
                                   get_online_users, real, register_view,
                                   render_template, time_diff, time_utcnow)
from flaskbb.utils.requirements import (CanAccessForum, CanAccessTopic,
                                        CanDeletePost, CanDeleteTopic,
                                        CanEditPost, CanPostReply,
                                        CanPostTopic, Has,
                                        IsAtleastModeratorInForum)
from flaskbb.utils.settings import flaskbb_config

impl = HookimplMarker('flaskbb')

logger = logging.getLogger(__name__)

class ApiIndex(MethodView):
    def get(self, username, password):
        return json.dumps({
            'username': username,
            'password': password
            })

@impl(tryfirst=True)
def flaskbb_load_blueprints(app):
    api = Blueprint("api", __name__)

    register_view(api, routes=['/<username>/<password>'], view_func=ApiIndex.as_view('index'))

    app.register_blueprint(api, url_prefix=app.config["API_URL_PREFIX"])
