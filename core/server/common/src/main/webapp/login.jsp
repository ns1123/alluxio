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
    <script src="js/md5.min.js"></script>
    <script src="js/jquery-1.9.1.min.js"></script>
</head>
<body>
<form id="loginForm" action="" method="POST">
    Username: <input id="username" type="text" name="username"/>
    <br/>

    <% if (request.getAttribute("usernameNotExists") != null) { %>
    <div id="usernameError">User does not exist.</div>
    <br/>
    <% } %>

    Password: <input id="password" type="password" name="password"/>
    <br/>

    <% if (request.getAttribute("passwordIncorrect") != null) { %>
    <div id="passwordError">Password is incorrect.</div>
    <br/>
    <% } %>

    <input type="submit" value="Login"/>
</form>

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
            success: function(data) {
                // Redirect to /home.
                window.location.replace("/home");
            },
            error: function(xhr) {
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
