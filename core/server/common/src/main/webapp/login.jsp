<%--

    The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
    (the "License"). You may not use this work except in compliance with the License, which is
    available at www.apache.org/licenses/LICENSE-2.0

    This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied, as more fully set forth in the License.

    See the NOTICE file distributed with this work for information regarding copyright ownership.

--%>

<html>
<head>
    <meta name="viewport" content="width=device-width">
    <link rel="stylesheet" href="css/bootstrap.min.css">
    <link rel="stylesheet" href="css/bootstrap-responsive.min.css">
    <link rel="stylesheet" href="css/custom.min.css">
    <script src="js/bootstrap.min.js"></script>
    <script src="js/md5.min.js"></script>
    <script src="js/jquery-1.9.1.min.js"></script>
</head>
<body>
<div class="container">
    <br/>
    <div class="span6 offset3">
        <div class="well">
            <div class="navbar text-center">
                <div class="navbar-inner">
                    <h3>&nbsp;<img style="height:25px;margin-top:-5px;" src="img/logo.png" alt="Alluxio Logo">&nbsp;</h3>
                </div>
            </div>
            <form id="loginForm" action="" method="POST" class="form-horizontal">
                <div class="control-group">
                    <label class="control-label" for="username">Username</label>
                    <div class="controls">
                        <input id="username" type="text" name="username" class="span3" placeholder="Username"/>
                    </div>
                </div>
                <div class="control-group">
                    <label class="control-label" for="password">Password</label>
                    <div class="controls">
                        <input id="password" type="password" name="password" class="span3" placeholder="Password"/>
                    </div>
                </div>
                <div class="control-group">
                <div class="controls">
                        <input type="submit" class="btn span3" value="Login"/>
                </div>
                <div class="controls">
                    <div class="text-error">
                        <% if (request.getAttribute("loginError") != null) { %>
                        <br/>
                        <h5 id="loginError">Login failed: user or password is incorrect.</h5>
                        <% } %>
                    </div>
                </div>
                </div>
            </form>
        </div>
    </div>
</div>

<script>
    $("#loginForm").submit(function (event) {
        event.preventDefault();
        var username = $("#username").val();
        var password = $("#password").val();
        var sessionID = "<%= session.getId() %>";
        var passwordHash = md5(password + " " + sessionID);
        var formData = {
            username: username,
            password: passwordHash
        };
        $.ajax({
            type: "POST",
            url: "/login",
            data: formData,
            success: function (data) {
                // Redirect to /home.
                window.location.replace("/home");
            },
            error: function (xhr) {
                // Replace the current HTML with the response which contains error messages.
                document.open();
                document.write(xhr.responseText);
                document.close();
            }
        });
    });
</script>
</body>
</html>
