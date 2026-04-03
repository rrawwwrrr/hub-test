#!/bin/bash
# Собирает .deb пакет для hub-test.
# Использование: bash packaging/build-deb.sh [version]
# Версия берётся из аргумента, GITHUB_REF_NAME или последнего git-тега.
set -euo pipefail

BINARY=${BINARY:-hub-test}
ARCH=${ARCH:-amd64}
APK=${APK:-}

# Определяем версию
VERSION=${1:-${GITHUB_REF_NAME:-}}
if [ -z "$VERSION" ]; then
    VERSION=$(git describe --tags --abbrev=0 2>/dev/null || echo "0.0.0")
fi
VERSION=${VERSION#v}  # убираем ведущий 'v'

PKG="hub-test_${VERSION}_${ARCH}"
echo "Building ${PKG}.deb ..."

# Создаём структуру пакета
rm -rf "${PKG}"
install -Dm755 "${BINARY}"                         "${PKG}/usr/local/bin/hub-test"
# Подставляем версию в TEST_IMAGE (latest → vX.Y.Z)
mkdir -p "${PKG}/etc/default"
sed "s|rrawwwrrr/hub-test-tests:latest|rrawwwrrr/hub-test-tests:${VERSION}|g" \
    packaging/hub-test.env > "${PKG}/etc/default/hub-test"
chmod 644 "${PKG}/etc/default/hub-test"
install -Dm644 packaging/hub-test.service           "${PKG}/etc/systemd/system/hub-test.service"
install -dm755                                     "${PKG}/var/lib/hub-test/apk"
install -dm755                                     "${PKG}/var/lib/hub-test/reports/logs"

# Включаем APK в пакет если передан через переменную APK=...
if [ -n "${APK}" ] && [ -f "${APK}" ]; then
    echo "→ Bundling APK: ${APK}"
    install -Dm644 "${APK}" "${PKG}/var/lib/hub-test/apk/$(basename "${APK}")"
fi

# DEBIAN/control
mkdir -p "${PKG}/DEBIAN"
cat > "${PKG}/DEBIAN/control" <<EOF
Package: hub-test
Version: ${VERSION}
Section: utils
Priority: optional
Architecture: ${ARCH}
Depends: docker.io | docker-ce, adb
Maintainer: rrawwwrrr
Description: ADB Test Runner с веб-дашбордом
 Следит за подключёнными Android-устройствами, запускает Appium
 и тестовые контейнеры в Docker, сохраняет результаты в SQLite
 и показывает их в браузере на порту 9080.
EOF

# DEBIAN/postinst — выполняется после установки
cat > "${PKG}/DEBIAN/postinst" <<'EOF'
#!/bin/bash
set -e
systemctl daemon-reload
systemctl enable hub-test

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║              hub-test успешно установлен                  ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  1. Отредактируй конфиг:                                 ║"
echo "║     nano /etc/default/hub-test                            ║"
echo "║                                                          ║"
echo "║  2. Запусти сервис:                                      ║"
echo "║     systemctl start hub-test                              ║"
echo "║                                                          ║"
echo "║  3. Логи:  journalctl -u hub-test -f                      ║"
echo "║  4. Дашборд: http://<ip>:9080                            ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
EOF
chmod 755 "${PKG}/DEBIAN/postinst"

# DEBIAN/prerm — выполняется перед удалением
cat > "${PKG}/DEBIAN/prerm" <<'EOF'
#!/bin/bash
set -e
systemctl stop hub-test    2>/dev/null || true
systemctl disable hub-test 2>/dev/null || true
EOF
chmod 755 "${PKG}/DEBIAN/prerm"

# Собираем пакет
dpkg-deb --build --root-owner-group "${PKG}"
rm -rf "${PKG}"

echo "Готово: ${PKG}.deb"
