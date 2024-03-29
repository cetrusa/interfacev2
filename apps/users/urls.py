#
from django.urls import path, reverse_lazy
from django.shortcuts import redirect


from django.contrib.auth.views import (
    PasswordResetView,
    PasswordResetDoneView,
    PasswordResetConfirmView,
    PasswordResetCompleteView,
)

from . import views

app_name = "users_app"

urlpatterns = [
    path(
        "login/",
        views.LoginUser.as_view(),
        name="user-login",
    ),
    # path('', lambda request: redirect('login/')),
    path("", views.home, name="home"),
    path(
        "logout/",
        views.LogoutView.as_view(),
        name="user-logout",
    ),
    path(
        "database/",
        views.DatabaseListView.as_view(),
        name="user-database",
    ),
    path(
        "base/",
        views.BaseView.as_view(),
        name="base",
    ),
    path("database/list/", views.database_list, name="database_list"),
    path(
        "register/",
        views.UserRegisterView.as_view(),
        name="user-register",
    ),
    path(
        "password_reset/",
        PasswordResetView.as_view(
            template_name="users/password_reset_form.html",
            email_template_name="users/password_reset_email.html",
            success_url=reverse_lazy("users_app:password_reset_done"),
        ),
        name="password_reset",
    ),
    path(
        "password_reset/done/",
        PasswordResetDoneView.as_view(template_name="users/password_reset_done.html"),
        name="password_reset_done",
    ),
    path(
        "reset/<uidb64>/<token>/",
        PasswordResetConfirmView.as_view(
            template_name="users/password_reset_confirm.html",
            success_url=reverse_lazy("users_app:password_reset_complete"),
        ),
        name="password_reset_confirm",
    ),
    path(
        "reset/done/",
        PasswordResetCompleteView.as_view(
            template_name="users/password_reset_complete.html"
        ),
        name="password_reset_complete",
    ),
]
