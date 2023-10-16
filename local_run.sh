docker-compose down -v
docker rmi new_admin_panel_sprint_3-transporter-1
docker rmi new_admin_panel_sprint_3-db-1
docker-compose up -d --build
